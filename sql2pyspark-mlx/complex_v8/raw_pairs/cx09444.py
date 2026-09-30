from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_all_by_ms_played_ties = Window.orderBy("ms_played")

filtered_plays = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) | (F.col("ms_played") >= 120663))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

metrics = (
    filtered_plays
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_ms_played), 3).alias("ms_played_percent_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id_track_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id_track_id"),
        F.rank().over(w_all_by_ms_played_ties).alias("ms_played_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id_track_id), 2).alias("ms_played_vs_listener_id_track_id_avg"),
    )
)

enriched_listener = (
    metrics.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.plan") == "family"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source_clean",
        "s.ms_played_percent_rank",
        "s.ms_played_rank",
        F.col("l.country").alias("listener_country"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.username").alias("listener_username"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

stats = (
    enriched_listener
    .groupBy(
        F.date_format("listener_signup_date", "yyyy-MM").alias("listener_signup_date_ym"),
    )
    .agg(
        F.sum(F.when(F.length(F.trim(F.col("listener_username"))) >= 7, F.col("ms_played"))).alias("ms_played_length_trim_listener_usernam"),
        F.sum(F.when(F.upper(F.trim(F.col("listener_username"))).like("G%"), 1).otherwise(0)).alias("n_upper_trim_listener_username"),
        F.sum("track_duration_sec").alias("total_track_duration_sec"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.min("listener_signup_date").alias("first_listener_signup_date"),
        F.max("played_at").alias("last_played_at"),
    )
)

scored = (
    stats
    .filter(F.col("total_track_duration_sec") <= 2034)
    .select(
        "listener_signup_date_ym",
        "ms_played_length_trim_listener_usernam",
        "total_track_duration_sec",
        "last_played_at",
        F.lower(F.trim(F.col("listener_signup_date_ym"))).alias("listener_signup_date_ym_clean"),
        F.when(F.col("median_ms_played") >= 281500, "large").when(F.col("median_ms_played") >= 103750, "medium").otherwise("small").alias("median_ms_played_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_played_at")).alias("days_since_last_played_at"),
    )
)

result = (
    scored
    .select(
        "listener_signup_date_ym",
        "ms_played_length_trim_listener_usernam",
        "total_track_duration_sec",
        "last_played_at",
        "listener_signup_date_ym_clean",
        "median_ms_played_tier",
        "days_since_last_played_at",
        F.concat(F.col("median_ms_played_tier"), F.lit("#"), F.col("total_track_duration_sec").cast("string")).alias("median_ms_played_tier_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("median_ms_played_tier")), " ", "_")).alias("median_ms_played_tier_slug"),
    )
    .orderBy(F.col("median_ms_played_tier").desc())
)
