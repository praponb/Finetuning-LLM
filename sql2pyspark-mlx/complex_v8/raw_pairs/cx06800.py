from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) | (F.col("bpm") < 143))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

filtered = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.size(F.col("t.moods")) >= 2) & (F.col("t.is_explicit")), "left_anti")
)

result = (
    filtered
    .groupingSets(
        [["artist_id", "is_explicit"], ["artist_id"], ["is_explicit"], []],
        "artist_id", "is_explicit",
    )
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_not_is_explicit"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.count("*").alias("n_rows"),
    )
)
