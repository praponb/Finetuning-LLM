from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_by_prev_played_at = Window.partitionBy("burst_id").orderBy(F.col("prev_played_at").desc_nulls_first(), "id")
w_burst_id_by_prev_played_at_t = Window.partitionBy("burst_id").orderBy(F.col("prev_played_at").desc_nulls_first())
w_is_new_session = Window.partitionBy("is_new_session")
w_burst_id_by_prev_played_at_f = Window.partitionBy("burst_id").orderBy(F.col("prev_played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_listener_id_running))

metrics = (
    sessionized
    .select(
        "*",
        F.lead("played_at").over(w_burst_id_by_prev_played_at).alias("next_played_at"),
        F.dense_rank().over(w_burst_id_by_prev_played_at).alias("prev_played_at_dense_rank"),
        F.rank().over(w_burst_id_by_prev_played_at_t).alias("prev_played_at_rank"),
        F.round(F.col("is_new_session") / F.nullif(F.sum("is_new_session").over(w_is_new_session), F.lit(0)), 4).alias("is_new_session_share_of_is_new_session"),
        F.last("played_at").over(w_burst_id_by_prev_played_at_f).alias("last_played_at"),
        F.size(F.collect_set("listener_id").over(w_is_new_session)).alias("distinct_listener_id_in_is_new_session"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "prev_played_at",
        "is_new_session",
        "burst_id",
        "prev_played_at_rank",
        "is_new_session_share_of_is_new_session",
        "distinct_listener_id_in_is_new_session",
    )
    .orderBy(F.col("id").desc(), "prev_played_at_rank")
)
