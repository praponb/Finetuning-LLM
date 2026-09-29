from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_by_bpm_bucket_fu = Window.partitionBy("is_explicit").orderBy(F.col("bpm_bucket").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm_minus_avg").desc(), "id")

prepared = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (~F.col("t.is_explicit")) & (F.col("t.release_date").between(F.lit("2024-08-26").cast("date"), F.lit("2025-04-02").cast("date"))), "left_anti")
)

qualified = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
    .filter(F.col("artist_id") <= 9)
)

artist_id_stats = (
    qualified
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

compared = (
    qualified.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

sequenced = (
    compared
    .select(
        "*",
        F.round(F.col("avg_bpm") - F.avg("avg_bpm").over(w_is_explicit), 2).alias("avg_bpm_vs_is_explicit_avg"),
        F.nth_value("bpm_minus_avg", 2).over(w_is_explicit_by_bpm_bucket_fu).alias("second_bpm_minus_avg"),
        F.first("bpm").over(w_is_explicit_by_bpm_bucket_fu).alias("first_bpm"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.ntile(3).over(w_is_explicit_artist_id_by_bpm).alias("bpm_minus_avg_tercile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "title_prefix",
        "bpm_bucket",
        "bpm_minus_avg",
        "avg_bpm_vs_is_explicit_avg",
        "second_bpm_minus_avg",
        "first_bpm",
        "distinct_artist_id_in_is_explicit",
        "rows_in_is_explicit",
        "bpm_minus_avg_tercile",
        (F.col("second_bpm_minus_avg") - F.col("bpm_bucket")).alias("second_bpm_minus_avg_minus_bpm_bucket"),
    )
    .orderBy(F.col("title_prefix").desc(), F.col("bpm_minus_avg").asc_nulls_last())
)
