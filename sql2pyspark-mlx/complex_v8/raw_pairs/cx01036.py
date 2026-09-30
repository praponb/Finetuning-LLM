from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

filtered = (
    df_tracks
    .filter(F.col("bpm") > 85)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

latest_per_key = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

artist_id_stats = (
    latest_per_key
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

compared = (
    latest_per_key.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

result = (
    compared
    .rollup("is_explicit", "artist_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.round(100 * F.count(F.when(~F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_not_is_explicit"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
