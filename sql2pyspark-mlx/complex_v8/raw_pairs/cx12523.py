from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.col("duration_sec") <= 357) & (F.col("release_date") < F.lit("2025-04-02").cast("date")))
)

filtered = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country") == "JP"), "left_anti")
)

filtered_2 = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id") <= 6), "left_semi")
)

per_group = (
    filtered_2
    .filter(F.col("release_date").isNotNull())
    .groupBy("is_explicit")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("is_explicit"), F.col("bpm")).otherwise(0)).alias("bpm_is_explicit_true"),
    )
)

result = (
    per_group
    .select(
        "is_explicit",
        "cnt",
        "bpm_is_explicit_true",
        F.greatest(F.col("cnt"), F.col("bpm_is_explicit_true")).alias("greatest_cnt_bpm_is_explicit_true"),
        (F.col("cnt") - F.col("bpm_is_explicit_true")).alias("cnt_minus_bpm_is_explicit_true"),
    )
    .orderBy(
        F.col("cnt").asc_nulls_last(),
        F.col("greatest_cnt_bpm_is_explicit_true").desc(),
    )
)
