from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 100).alias("bpm_distance"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec") >= 142) & (F.array_contains(F.col("t.moods"), "party")), "left_semi")
)

scored = (
    without_matches
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        "bpm_distance",
        "is_explicit_label",
        F.concat_ws("-", F.col("title"), F.col("is_explicit_label")).alias("title_is_explicit_label_label"),
        F.lower(F.regexp_replace(F.trim(F.col("is_explicit_label")), " ", "_")).alias("is_explicit_label_slug"),
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

result = (
    scored
    .rollup("is_explicit", "artist_id")
    .agg(F.sum("bpm").alias("total_bpm"))
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
