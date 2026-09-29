from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id = Window.partitionBy("session_id")
w_new_session_flag_closed_at_m = Window.partitionBy("new_session_flag", F.month("closed_at")).orderBy("closed_at")
w_new_session_flag_closed_at_m_2 = Window.partitionBy("new_session_flag", F.month("closed_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tickets = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.when(F.col("channel").isin("phone", "web"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
    )
)

with_prev = filtered_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_priority_running))

with_windows = (
    sessionized
    .select(
        "id",
        "requester_id",
        "closed_at",
        "channel",
        "new_session_flag",
        "session_id",
        F.size(F.collect_set("priority").over(w_session_id)).alias("distinct_priority_in_session_id"),
        F.rank().over(w_new_session_flag_closed_at_m).alias("closed_at_rank"),
        F.round(F.col("new_session_flag") / F.nullif(F.sum("new_session_flag").over(w_session_id), F.lit(0)), 4).alias("new_session_flag_share_of_session_id"),
        F.nth_value("prev_closed_at", 2).over(w_new_session_flag_closed_at_m_2).alias("second_prev_closed_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "closed_at",
        "new_session_flag",
        "distinct_priority_in_session_id",
        "closed_at_rank",
        "new_session_flag_share_of_session_id",
        "second_prev_closed_at",
        F.when(F.col("channel").isin("phone", "email"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
    )
    .orderBy(F.col("closed_at_rank").asc())
)
