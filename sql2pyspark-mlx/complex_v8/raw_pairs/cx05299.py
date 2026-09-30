from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played_ties = Window.partitionBy("track_id").orderBy("ms_played")
w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")

valid_plays = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(2, 3))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

ranked = (
    valid_plays
    .select(
        "*",
        F.round(F.percent_rank().over(w_track_id_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_is_skipped_track_id), F.lit(0)), 4).alias("ms_played_share_of_is_skipped_track_id"),
        F.round(F.cume_dist().over(w_track_id_by_ms_played_ties), 3).alias("ms_played_cume_dist"),
    )
)

joined_listener = (
    ranked.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
        F.col("l.interests").alias("listener_interests"),
    )
)

result = (
    joined_listener
    .select("id", "ms_played_percent_rank", "ms_played_cume_dist")
    .unpivot(
        ids=["id"],
        values=["ms_played_percent_rank", "ms_played_cume_dist"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
