from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_track_id = Window.partitionBy(F.month("played_at"), "track_id")
w_device_by_ms_played_bucket_p = Window.partitionBy("device").orderBy("ms_played_bucket", "id").rowsBetween(-1, -1)
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_listener_id_track_id_by_ms_p = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc(), "id")

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

sequenced = (
    valid_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "played_at_plus_15m",
        F.min("ms_played_bucket").over(w_played_at_month_track_id).alias("played_at_month_track_id_min_ms_played_bucket"),
        F.round(F.col("ms_played_bucket") / F.nullif(F.sum("ms_played_bucket").over(w_played_at_month_track_id), F.lit(0)), 4).alias("ms_played_bucket_share_of_played_at_month_track_"),
        F.round(F.avg("ms_played_bucket").over(w_device_by_ms_played_bucket_p), 2).alias("ms_played_bucket_avg_1_prior"),
        F.rank().over(w_all_by_ms_played).alias("ms_played_rank"),
        F.row_number().over(w_listener_id_track_id_by_ms_p).alias("seq_no"),
        F.lag("played_at").over(w_all_by_ms_played).alias("prev_played_at"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.played_at_plus_15m",
        "s.seq_no",
        "s.prev_played_at",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

qualified = (
    combined_details.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

result = (
    qualified
    .cube("listener_plan", "listener_country", "is_skipped")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("played_at_plus_15m").alias("first_played_at_plus_15m"),
        F.sum(F.when(~F.col("listener_country").isin("UK", "JP"), F.col("seq_no")).otherwise(0)).alias("seq_no_listener_country_not_in_uk_j"),
    )
)
