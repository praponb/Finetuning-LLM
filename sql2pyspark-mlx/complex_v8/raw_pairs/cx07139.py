from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_week_month_arti = Window.partitionBy(F.month("release_date_week"), "artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_filled_prio = Window.partitionBy("artist_id").orderBy(F.col("bpm_filled").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_bpm_cume_dist = Window.orderBy("bpm_cume_dist", "id")
w_artist_id_by_bpm_filled = Window.partitionBy("artist_id").orderBy(F.col("bpm_filled").asc_nulls_last(), "id")
w_artist_id_by_bpm_filled_ties = Window.partitionBy("artist_id").orderBy(F.col("bpm_filled").asc_nulls_last())
w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm_cume_dist_share_of_artist_id").desc(), F.col("id").desc())

valid_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (F.col("release_date") < F.lit("2025-08-02").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

tagged = (
    valid_tracks
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.abs(F.col("bpm") - 127).alias("bpm_distance"),
    )
)

sequenced = (
    tagged
    .withColumn("bpm_cume_dist", F.round(F.cume_dist().over(w_release_date_week_month_arti), 3))
    .withColumn("rows_in_artist_id", F.count(F.lit(1)).over(w_artist_id))
)

windowed = (
    sequenced
    .select(
        "*",
        F.round(F.avg("bpm_distance").over(w_artist_id_by_bpm_filled_prio), 2).alias("bpm_distance_avg_2_prior"),
        F.ntile(5).over(w_all_by_bpm_cume_dist).alias("bpm_cume_dist_quintile"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.row_number().over(w_artist_id_by_bpm_filled).alias("seq_no"),
        F.round(F.percent_rank().over(w_artist_id_by_bpm_filled_ties), 3).alias("bpm_filled_percent_rank"),
        F.round(F.col("bpm_cume_dist") / F.nullif(F.sum("bpm_cume_dist").over(w_artist_id), F.lit(0)), 4).alias("bpm_cume_dist_share_of_artist_id"),
    )
)

unique_rows = (
    windowed
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

artist_id_stats = (
    unique_rows
    .groupBy("artist_id")
    .agg(F.avg("bpm_distance").alias("avg_bpm_distance"))
)

outliers = (
    unique_rows.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_distance") > F.col("g.avg_bpm_distance"))
    .select(
        "s.*",
        "g.avg_bpm_distance",
        F.round(F.col("s.bpm_distance") - F.col("g.avg_bpm_distance"), 2).alias("bpm_distance_minus_avg"),
    )
)

screened = (
    outliers.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date") >= F.lit("2024-06-03").cast("date")) & (F.col("t.duration_sec") < 402), "left_anti")
)

result = (
    screened
    .filter(F.col("bpm") >= 96)
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        "release_date_week",
        "bpm_filled",
        "bpm_distance",
        "bpm_cume_dist",
        "rows_in_artist_id",
        "bpm_cume_dist_quintile",
        "bpm_vs_artist_id_avg",
        "seq_no",
        "bpm_cume_dist_share_of_artist_id",
        "avg_bpm_distance",
    )
    .orderBy("bpm_cume_dist_quintile", "avg_bpm_distance")
)
