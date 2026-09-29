from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_listener_signup_date = Window.orderBy("listener_signup_date", "id")
w_latest = Window.partitionBy("artist_genre", "source").orderBy(F.col("listener_signup_date_cume_dist").desc(), F.col("id").desc())

valid = (
    df_plays
    .filter((F.col("is_skipped")) | (F.col("ms_played") < 326250))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

enriched_track = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_or_unknown",
        "s.device_or_unknown",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

enriched = (
    enriched_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "track_bpm",
        "track_artist_id",
        F.when(F.col("track_bpm") >= 143, "HIGH").when(F.col("track_bpm") >= 94, "MEDIUM").otherwise("LOW").alias("track_bpm_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("source_or_unknown")), " ", "_")).alias("source_or_unknown_slug"),
    )
)

enriched_artist = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.track_artist_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.track_artist_id",
        "s.track_bpm_tier",
        "s.source_or_unknown_slug",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

metrics = (
    enriched_artist
    .withColumn("listener_signup_date_cume_dist", F.round(F.cume_dist().over(w_all_by_listener_signup_date), 3))
    .withColumn("listener_birth_year_change", F.col("listener_birth_year") - F.lag("listener_birth_year").over(w_all_by_listener_signup_date))
)

deduped = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.max("ms_played").alias("max_play_ms_played"),
    )
)

summarized_rows = (
    deduped.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.max_play_ms_played",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "artist_debut_year",
        "listener_birth_year_change",
        "listener_birth_year",
    )
    .unpivot(
        ids=["id"],
        values=["artist_debut_year", "listener_birth_year_change", "listener_birth_year"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
