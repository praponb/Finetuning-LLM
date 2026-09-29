from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))
w_agent_id_session_id_by_opene = Window.partitionBy("agent_id", "session_id").orderBy("opened_at")

scoped = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("priority").isin("P1", "P2"), "group_a").when(F.col("priority") == "P4", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_prev = scoped.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_channel_running))

enriched_agent = (
    sessionized.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on") >= F.lit("2024-05-02").cast("date")), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.priority_group",
        "s.prev_opened_at",
        "s.session_id",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    enriched_agent
    .select(
        "id",
        "opened_at",
        "tags",
        "prev_opened_at",
        "session_id",
        F.round(F.col("session_id") / F.nullif(F.sum("session_id").over(w_closed_at_to_date), F.lit(0)), 4).alias("session_id_share_of_closed_at_to_date"),
        F.round(F.cume_dist().over(w_agent_id_session_id_by_opene), 3).alias("opened_at_cume_dist"),
        F.size(F.collect_set("channel").over(w_closed_at_to_date)).alias("distinct_channel_in_closed_at_to_date"),
    )
)

result = (
    sequenced
    .select("id", "session_id_share_of_closed_at_to_date", "opened_at_cume_dist")
    .unpivot(
        ids=["id"],
        values=["session_id_share_of_closed_at_to_date", "opened_at_cume_dist"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
