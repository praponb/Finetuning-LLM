from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_distance").desc(), F.col("id").desc())

recent_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

qualified = (
    recent_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.is_skipped")) & (F.col("p.played_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")), "left_anti")
)

tagged = (
    qualified
    .select(
        "*",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.abs(F.col("duration_sec") - 344).alias("duration_sec_distance"),
    )
)

without_matches = (
    tagged
    .crossJoin(tagged.agg(F.avg("duration_sec").alias("overall_avg_duration_sec")))
    .filter(F.col("duration_sec") > F.col("overall_avg_duration_sec"))
    .drop("overall_avg_duration_sec")
)

latest_per_key = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    latest_per_key
    .select(
        "id",
        "release_date",
        "is_explicit",
        "is_explicit_label",
        "days_since_release_date",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.least(F.col("duration_sec"), F.col("days_since_release_date")).alias("least_duration_sec_days_since_release_date"),
    )
)

result = (
    labeled
    .groupBy("release_date_month_start")
    .pivot("is_explicit_label", ["no", "yes"])
    .agg(F.max("days_since_release_date"))
    .orderBy("release_date_month_start")
)
