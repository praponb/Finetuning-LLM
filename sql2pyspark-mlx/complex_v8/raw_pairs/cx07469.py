from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_max_ms_played = Window.partitionBy("is_skipped").orderBy("max_ms_played", "id")
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_is_skipped_by_max_ms_played_2 = Window.partitionBy("is_skipped").orderBy("max_ms_played")

recent_plays = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
    )
)

listener_id_stats = (
    recent_plays
    .groupBy("listener_id")
    .agg(F.max("ms_played").alias("max_ms_played"))
)

outliers = (
    recent_plays.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") == F.col("g.max_ms_played"))
    .select(
        "s.*",
        "g.max_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.max_ms_played"), 2).alias("ms_played_minus_max"),
    )
)

windowed = (
    outliers
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "max_ms_played",
        "ms_played_minus_max",
        F.round(F.percent_rank().over(w_is_skipped_by_max_ms_played), 3).alias("max_ms_played_percent_rank"),
        F.round(F.col("ms_played_minus_max") / F.nullif(F.sum("ms_played_minus_max").over(w_track_id_listener_id), F.lit(0)), 4).alias("ms_played_minus_max_share_of_track_id_listener_i"),
        F.round(F.cume_dist().over(w_is_skipped_by_max_ms_played_2), 3).alias("max_ms_played_cume_dist"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.ms_played_minus_max",
        "s.max_ms_played_percent_rank",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_details = (
    with_parent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year_2"),
        F.col("l.username").alias("listener_username"),
    )
)

filtered = (
    with_details.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id").isNotNull()) & (F.col("p.source") == "radio"), "left_semi")
)

summary = (
    filtered
    .groupBy(
        "listener_country",
        "is_skipped",
        F.date_format("listener_signup_date", "yyyy-MM").alias("listener_signup_date_ym"),
    )
    .agg(
        F.sum("ms_played").alias("total_ms_played"),
        F.count("*").alias("cnt"),
        F.sum("listener_birth_year").alias("total_listener_birth_year"),
        F.countDistinct("listener_plan").alias("distinct_listener_plans"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.sum("listener_birth_year_2").alias("total_listener_birth_year_2"),
    )
)

result = (
    summary
    .select(
        "listener_country",
        "is_skipped",
        "listener_signup_date_ym",
        "total_ms_played",
        "cnt",
        "total_listener_birth_year",
        "distinct_listener_plans",
        "median_ms_played",
        F.coalesce(F.col("listener_country"), F.lit("UNKNOWN")).alias("listener_country_or_unknown"),
    )
    .orderBy(F.col("listener_signup_date_ym").asc_nulls_last())
)
