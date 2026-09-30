from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

prepared_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.upper(F.trim(F.col("title"))).like("E%")))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
    )
)

without_matches = (
    prepared_tracks.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

deduped = (
    without_matches
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    deduped
    .filter(~F.array_contains(F.col("moods"), "workout"))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 142, "A").when(F.col("bpm") >= 96, "B").otherwise("C").alias("bpm_tier"),
    )
)

rollup_base = (
    labeled
    .filter(F.col("artist_id").isNotNull())
    .groupBy("bpm_tier", "artist_id")
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.countDistinct("bpm_tier").alias("distinct_bpm_tiers"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

detail_rows = (
    rollup_base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.bpm_tier",
        "s.artist_id",
        "s.distinct_bpm_tiers",
        "s.n_rows",
        F.col("t.id").alias("track_id"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    detail_rows
    .select(
        "bpm_tier",
        "distinct_bpm_tiers",
        "n_rows",
        "track_id",
        "track_bpm",
        "track_duration_sec",
        F.array_contains(F.col("track_moods"), "party").alias("has_party"),
        F.greatest(F.col("n_rows"), F.col("track_duration_sec")).alias("greatest_n_rows_track_duration_sec"),
    )
    .orderBy(
        F.col("greatest_n_rows_track_duration_sec").desc_nulls_first(),
        F.col("n_rows").asc(),
    )
)
