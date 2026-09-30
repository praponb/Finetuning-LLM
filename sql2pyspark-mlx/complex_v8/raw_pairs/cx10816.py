from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_no_by_requester_id = Window.partitionBy("session_no").orderBy(F.col("requester_id").desc(), "id")
w_session_no_by_requester_id_m = Window.partitionBy("session_no").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-1, Window.currentRow)

valid_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

with_prev = valid_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_priority_running))

with_trends = (
    sessionized
    .select(
        "*",
        F.ntile(5).over(w_session_no_by_requester_id).alias("requester_id_quintile"),
        F.sum("is_new_session").over(w_session_no_by_requester_id_m).alias("is_new_session_sum_2"),
    )
)

with_details_agent = (
    with_trends.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel_slug",
        "s.prev_opened_at",
        "s.is_new_session",
        "s.session_no",
        "s.requester_id_quintile",
        "s.is_new_session_sum_2",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel_slug",
        "is_new_session",
        "requester_id_quintile",
        "is_new_session_sum_2",
        "agent_tier",
        "agent_team",
    )
    .orderBy(F.col("agent_team").desc_nulls_first(), F.col("id").desc_nulls_first())
)
