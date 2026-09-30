from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm_minus_avg").desc(), "id").rowsBetween(-6, -1)
w_all_by_duration_sec_moving_6 = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_avg_bpm_full = Window.orderBy(F.col("avg_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm_minus_avg").desc(), "id").rowsBetween(-3, -1)
w_all_by_avg_bpm = Window.orderBy(F.col("avg_bpm").desc(), "id")

recent_tracks = (
    df_tracks
    .filter(F.col("duration_sec") <= 357)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.when(F.col("duration_sec") >= 352, "large").when(F.col("duration_sec") >= 183, "medium").otherwise("small").alias("duration_sec_tier"),
        F.year(F.col("release_date")).alias("release_date_year"),
    )
)

artist_id_stats = (
    recent_tracks
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

above_average = (
    recent_tracks.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

ranked = (
    above_average
    .select(
        "*",
        F.max("duration_sec").over(w_artist_id_is_explicit_by_bpm).alias("duration_sec_max_6_prior"),
        F.max("bpm_minus_avg").over(w_all_by_duration_sec_moving_6).alias("bpm_minus_avg_max_7"),
        F.first("duration_sec").over(w_all_by_avg_bpm_full).alias("first_duration_sec"),
        F.round(F.avg("bpm").over(w_artist_id_is_explicit_by_bpm_2), 2).alias("bpm_avg_3_prior"),
        F.round(F.percent_rank().over(w_all_by_avg_bpm), 3).alias("avg_bpm_percent_rank"),
    )
)

result = (
    ranked
    .cube("artist_id", "duration_sec_tier", "release_date_year")
    .agg(F.max("bpm_minus_avg").alias("max_bpm_minus_avg"))
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("duration_sec_tier").asc_nulls_last(),
        F.col("release_date_year").asc_nulls_last(),
    )
)
