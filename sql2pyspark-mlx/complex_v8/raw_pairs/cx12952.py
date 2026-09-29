from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_played_at = Window.orderBy("days_since_played_at")
w_device_track_id = Window.partitionBy("device", "track_id")
w_all_by_days_since_played_at_2 = Window.orderBy("days_since_played_at", "id")

base = (
    df_plays
    .filter((F.col("track_id") <= 23) & (F.col("ms_played").between(95960, 236512)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_days_since_played_at), 3).alias("days_since_played_at_percent_rank"),
        F.sum("ms_played").over(w_device_track_id).alias("device_track_id_sum_ms_played"),
        F.round(F.percent_rank().over(w_all_by_days_since_played_at_2), 3).alias("days_since_played_at_percent_rank_2"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left_semi")
)

combined_details = (
    without_matches.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.interests").alias("listener_interests"),
    )
)

per_label = (
    combined_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "days_since_played_at",
        "days_since_played_at_percent_rank",
        "device_track_id_sum_ms_played",
        "days_since_played_at_percent_rank_2",
        "listener_signup_date",
        F.explode_outer("listener_interests").alias("listener_interest"),
    )
)

result = (
    per_label
    .groupBy("track_id")
    .pivot("listener_interest", ["jazz", "live", "kpop"])
    .agg(F.count("days_since_played_at_percent_rank"))
    .orderBy("track_id")
)
