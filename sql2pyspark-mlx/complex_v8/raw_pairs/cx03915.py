from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())
w_n_rows_distinct_ids_tier_by = Window.partitionBy("n_rows", "distinct_ids_tier").orderBy(F.col("n_rows").desc(), "artist_id", "last_played_at_month")
w_n_rows = Window.partitionBy("n_rows")
w_n_rows_distinct_ids_tier_by_2 = Window.partitionBy("n_rows", "distinct_ids_tier").orderBy(F.col("n_rows").desc())
w_all_by_n_rows = Window.orderBy("n_rows", "artist_id", "last_played_at_month")

filtered_tracks = (
    df_tracks
    .filter((F.col("duration_sec") < 365) & (F.col("release_date").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

latest = (
    filtered_tracks
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_stats = (
    latest.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at")
)

stats = (
    with_stats
    .groupBy(
        "artist_id",
        F.date_trunc("month", "last_played_at").alias("last_played_at_month"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("total_duration_sec") > 144)
)

tagged = (
    stats
    .filter(F.col("last_played_at_month") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .select(
        "artist_id",
        "last_played_at_month",
        "n_rows",
        F.when(F.col("distinct_ids") >= 8, "A").when(F.col("distinct_ids") >= 5, "B").otherwise("C").alias("distinct_ids_tier"),
        (F.col("n_rows") - F.col("distinct_ids")).alias("n_rows_minus_distinct_ids"),
        F.to_date(F.col("last_played_at_month")).alias("last_played_at_month_date"),
    )
)

ranked = (
    tagged
    .select(
        "artist_id",
        "last_played_at_month",
        "n_rows",
        "n_rows_minus_distinct_ids",
        "last_played_at_month_date",
        F.row_number().over(w_n_rows_distinct_ids_tier_by).alias("rn"),
        F.ntile(5).over(w_n_rows_distinct_ids_tier_by).alias("n_rows_quintile"),
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_n_rows), 2).alias("n_rows_vs_n_rows_avg"),
        F.round(F.percent_rank().over(w_n_rows_distinct_ids_tier_by_2), 3).alias("n_rows_percent_rank"),
        (F.col("n_rows_minus_distinct_ids") - F.lag("n_rows_minus_distinct_ids").over(w_all_by_n_rows)).alias("n_rows_minus_distinct_ids_change"),
        F.count(F.lit(1)).over(w_n_rows).alias("rows_in_n_rows"),
    )
)

without_matches = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

result = (
    without_matches
    .filter(F.col("n_rows_minus_distinct_ids_change") >= -4)
    .select(
        "artist_id",
        "last_played_at_month",
        "n_rows",
        "n_rows_minus_distinct_ids",
        "last_played_at_month_date",
        "rn",
        "n_rows_quintile",
        "n_rows_percent_rank",
        "rows_in_n_rows",
    )
    .orderBy(
        F.col("rn").asc(),
        F.col("last_played_at_month_date").asc_nulls_last(),
        "artist_id",
        "last_played_at_month",
    )
    .limit(5)
)
