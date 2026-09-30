from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter(F.col("source") != "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

annotated = (
    prepared
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "played_at_plus_1d",
        "played_at_label",
        F.concat(F.col("played_at_label"), F.lit("#"), F.col("listener_id").cast("string")).alias("played_at_label_ref"),
        F.date_trunc("quarter", F.col("played_at_date")).alias("played_at_date_quarter"),
        F.substring(F.trim(F.col("played_at_label")), 1, 4).alias("played_at_label_prefix"),
    )
)

with_details = (
    annotated.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .filter(F.col("t.duration_sec") < 312)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.played_at_label",
        "s.played_at_label_ref",
        F.col("t.title").alias("track_title"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

result = (
    with_details
    .filter(F.col("played_at_label").isNotNull())
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "source",
        "played_at_label",
        "played_at_label_ref",
        "track_title",
        "track_duration_sec",
        "artist_debut_year",
        "artist_name",
        F.length(F.trim(F.col("played_at_label_ref"))).alias("played_at_label_ref_len"),
    )
)
