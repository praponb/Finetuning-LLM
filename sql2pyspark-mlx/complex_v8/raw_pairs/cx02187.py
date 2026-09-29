from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")

scoped = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-04-29").cast("date"), F.lit("2025-08-02").cast("date"))) & (F.col("bpm") <= 142))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

artist_id_stats = (
    scoped
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

above_average = (
    scoped.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

enriched = (
    above_average
    .filter(F.col("bpm_minus_avg") < 54.0)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_filled",
        "avg_bpm",
        F.array_contains(F.col("moods"), "sad").alias("has_sad"),
        F.round(F.col("avg_bpm") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("avg_bpm_per_bpm"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "duration_sec_filled",
        "has_sad",
        "avg_bpm_per_bpm",
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.min("bpm").over(w_all_by_bpm_running).alias("min_bpm_so_far"),
        F.max("bpm").over(w_is_explicit).alias("is_explicit_max_bpm"),
        F.min("avg_bpm_per_bpm").over(w_is_explicit).alias("is_explicit_min_avg_bpm_per_bpm"),
        F.lag("duration_sec").over(w_all_by_bpm).alias("prev_duration_sec"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "duration_sec_filled",
        "has_sad",
        "rows_in_is_explicit",
        "min_bpm_so_far",
        "is_explicit_max_bpm",
        "prev_duration_sec",
        (F.col("min_bpm_so_far") - F.col("avg_bpm_per_bpm")).alias("min_bpm_so_far_minus_avg_bpm_per_bpm"),
        (F.col("is_explicit_max_bpm") + F.col("rows_in_is_explicit")).alias("is_explicit_max_bpm_plus_rows_in_is_explicit"),
    )
    .orderBy(F.col("min_bpm_so_far").asc_nulls_last(), "id")
    .limit(20)
)
