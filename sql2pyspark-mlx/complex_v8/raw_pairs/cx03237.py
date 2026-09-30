from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_track_id_by = Window.partitionBy(F.month("played_at"), "track_id").orderBy(F.col("ms_played").desc(), "id")
w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at_prior_2 = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(-2, -1)
w_artist_is_verified_ms_played = Window.partitionBy("artist_is_verified", "ms_played_decile_tier")
w_track_release_date_year_arti = Window.partitionBy(F.year("track_release_date"), "artist_country").orderBy("track_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_by_ms_pla = Window.partitionBy("artist_is_verified").orderBy("ms_played_share_of_listener_id", "id").rowsBetween(-6, -1)
w_artist_is_verified_by_ms_pla_2 = Window.partitionBy("artist_is_verified").orderBy("ms_played_share_of_listener_id", "id")

scoped_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("ms_played") >= 97825))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

metrics = (
    scoped_plays
    .select(
        "*",
        F.ntile(10).over(w_played_at_month_track_id_by).alias("ms_played_decile"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.sum("ms_played").over(w_all_by_played_at_prior_2).alias("ms_played_sum_2_prior"),
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
    )
)

with_parent_track = (
    metrics.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.ms_played_decile",
        "s.ms_played_share_of_listener_id",
        "s.ms_played_sum_2_prior",
        "s.listener_id_min_ms_played",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.title").alias("track_title"),
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

enriched = (
    with_parent_track
    .select(
        "id",
        "played_at",
        "ms_played_decile",
        "ms_played_share_of_listener_id",
        "ms_played_sum_2_prior",
        "listener_id_min_ms_played",
        "track_release_date",
        "artist_country",
        "artist_is_verified",
        F.trunc(F.col("track_release_date"), "MM").alias("track_release_date_month_start"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

without_matches = (
    enriched
    .crossJoin(enriched.agg(F.max("days_since_played_at").alias("overall_max_days_since_played_at")))
    .filter(F.col("days_since_played_at") < F.col("overall_max_days_since_played_at"))
    .drop("overall_max_days_since_played_at")
)

annotated = (
    without_matches
    .filter(F.col("ms_played_sum_2_prior") <= 961000)
    .select(
        "id",
        "played_at",
        "ms_played_share_of_listener_id",
        "ms_played_sum_2_prior",
        "listener_id_min_ms_played",
        "track_release_date",
        "artist_country",
        "artist_is_verified",
        "track_release_date_month_start",
        (F.floor(F.col("days_since_played_at") / 100) * 100).alias("days_since_played_at_bucket"),
        F.when(F.col("ms_played_decile") >= 7, "large").when(F.col("ms_played_decile") >= 4, "medium").otherwise("small").alias("ms_played_decile_tier"),
    )
)

metrics_2 = (
    annotated
    .select(
        "id",
        "played_at",
        "ms_played_sum_2_prior",
        "listener_id_min_ms_played",
        "track_release_date",
        "artist_country",
        "artist_is_verified",
        "track_release_date_month_start",
        "ms_played_decile_tier",
        F.sum("days_since_played_at_bucket").over(w_artist_is_verified_ms_played).alias("artist_is_verified_ms_played_decile_tier_sum_day"),
        F.sum("ms_played_sum_2_prior").over(w_track_release_date_year_arti).alias("running_ms_played_sum_2_prior"),
        F.round(F.avg("listener_id_min_ms_played").over(w_artist_is_verified_by_ms_pla), 2).alias("listener_id_min_ms_played_avg_6_prior"),
        F.ntile(5).over(w_artist_is_verified_by_ms_pla_2).alias("ms_played_share_of_listener_id_quintile"),
    )
)

result = (
    metrics_2
    .filter(F.col("listener_id_min_ms_played_avg_6_prior") > 404500.0)
    .select(
        "id",
        "ms_played_sum_2_prior",
        "listener_id_min_ms_played",
        "track_release_date",
        "artist_country",
        "artist_is_verified",
        "artist_is_verified_ms_played_decile_tier_sum_day",
        "listener_id_min_ms_played_avg_6_prior",
    )
    .orderBy(
        "ms_played_sum_2_prior",
        F.col("listener_id_min_ms_played_avg_6_prior").desc(),
    )
)
