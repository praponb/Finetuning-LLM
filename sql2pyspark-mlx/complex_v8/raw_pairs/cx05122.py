from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("avg_played_at_year_tier", "listener_id").orderBy(F.col("played_at_year").desc(), F.col("id").desc())

valid = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("source").isin("playlist", "radio", "search")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.year(F.col("played_at")).alias("played_at_year"),
    )
)

device_stats = (
    valid
    .groupBy("device")
    .agg(F.avg("played_at_year").alias("avg_played_at_year"))
)

compared = (
    valid.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.played_at_year") > F.col("g.avg_played_at_year"))
    .select(
        "s.*",
        "g.avg_played_at_year",
        F.round(F.col("s.played_at_year") - F.col("g.avg_played_at_year"), 2).alias("played_at_year_minus_avg"),
    )
)

enriched = (
    compared
    .select(
        "*",
        F.when(F.col("avg_played_at_year") >= 2025.4, "HIGH").when(F.col("avg_played_at_year") >= 2024.9, "MEDIUM").otherwise("LOW").alias("avg_played_at_year_tier"),
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
        F.greatest(F.col("played_at_year"), F.col("avg_played_at_year")).alias("greatest_played_at_year_avg_played_at_year"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_track = (
    deduped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_year",
        "s.avg_played_at_year",
        "s.played_at_year_minus_avg",
        "s.avg_played_at_year_tier",
        "s.played_at_plus_15m",
        "s.greatest_played_at_year_avg_played_at_year",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.moods").alias("track_moods"),
    )
)

annotated = (
    with_details_track
    .filter(F.col("track_is_explicit"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "played_at_year",
        "avg_played_at_year",
        "played_at_year_minus_avg",
        "played_at_plus_15m",
        "greatest_played_at_year_avg_played_at_year",
        (F.floor(F.col("played_at_year_minus_avg") / 1000) * 1000).alias("played_at_year_minus_avg_bucket"),
    )
)

with_parent_track = (
    annotated.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(~F.col("t.is_explicit"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.played_at_year",
        "s.avg_played_at_year",
        "s.played_at_year_minus_avg",
        "s.greatest_played_at_year_avg_played_at_year",
        "s.played_at_year_minus_avg_bucket",
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

result = (
    with_parent_track
    .filter(F.col("greatest_played_at_year_avg_played_at_year") < 2025.8)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "played_at_year",
        "avg_played_at_year",
        "played_at_year_minus_avg",
        "played_at_year_minus_avg_bucket",
        "track_moods",
        "listener_signup_date",
    )
    .orderBy(F.col("played_at").desc_nulls_first(), "id")
    .limit(10)
)
