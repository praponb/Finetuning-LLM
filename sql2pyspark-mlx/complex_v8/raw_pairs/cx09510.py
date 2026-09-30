from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_week = Window.partitionBy("artist_id", F.year("release_date_week")).orderBy(F.col("duration_sec_minus_bpm").desc(), "id")
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date_week").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tracks
    .filter((F.col("bpm") <= 166) & (F.lower(F.col("title")).like("%ur%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

metrics = (
    filtered
    .withColumn("duration_sec_minus_bpm_percent_rank", F.round(F.percent_rank().over(w_artist_id_release_date_week), 3))
    .withColumn("release_date_month_min_least_bpm_duration_sec", F.min("least_bpm_duration_sec").over(w_release_date_month))
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.nth_value("release_date", 2).over(w_is_explicit_by_release_date).alias("second_release_date"),
        F.round(F.col("least_bpm_duration_sec") / F.nullif(F.sum("least_bpm_duration_sec").over(w_artist_id), F.lit(0)), 4).alias("least_bpm_duration_sec_share_of_artist_id"),
        F.max("least_bpm_duration_sec").over(w_artist_id).alias("artist_id_max_least_bpm_duration_sec"),
    )
)

with_parent = (
    metrics_2.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.least_bpm_duration_sec",
        "s.duration_sec_minus_bpm",
        "s.duration_sec_minus_bpm_percent_rank",
        "s.release_date_month_min_least_bpm_duration_sec",
        "s.bpm_vs_artist_id_avg",
        "s.second_release_date",
        "s.least_bpm_duration_sec_share_of_artist_id",
        "s.artist_id_max_least_bpm_duration_sec",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    with_parent
    .rollup("artist_id", "artist_country")
    .agg(
        F.max("second_release_date").alias("last_second_release_date"),
        F.min("release_date").alias("first_release_date"),
    )
    .select(
        "artist_id",
        F.coalesce(F.col("artist_country"), F.lit("ALL")).alias("artist_country"),
        "last_second_release_date",
        "first_release_date",
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)
