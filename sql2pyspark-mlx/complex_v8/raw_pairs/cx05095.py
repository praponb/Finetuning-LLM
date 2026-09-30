from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
        F.year(F.col("played_at")).alias("played_at_year"),
    )
)

source_stats = (
    valid_plays
    .groupBy("source")
    .agg(F.avg("played_at_year").alias("avg_played_at_year"))
)

vs_group = (
    valid_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.played_at_year") > F.col("g.avg_played_at_year"))
    .select(
        "s.*",
        "g.avg_played_at_year",
        F.round(F.col("s.played_at_year") - F.col("g.avg_played_at_year"), 2).alias("played_at_year_minus_avg"),
    )
)

with_parent_listener = (
    vs_group.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.upper(F.trim(F.col("l.username"))).like("F%")), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.source_ref",
        "s.played_at_year_minus_avg",
        F.col("l.country").alias("listener_country"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    with_parent_listener
    .groupingSets(
        [["track_id", "source"], ["track_id"], ["source"], []],
        "track_id", "source",
    )
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.sum(F.when(F.col("listener_country") == "KR", 1).otherwise(0)).alias("n_listener_country_kr"),
    )
)
