from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("csat").desc(), F.col("id").desc())

prepared = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) | (F.col("opened_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")))
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
    )
)

latest = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_agent = (
    latest.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

without_matches = (
    with_details_agent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("assigned", "resolved")), "left_semi")
    .filter(F.col("opened_at") >= F.lit("2025-01-13 07:00:00").cast("timestamp"))
)

result = (
    without_matches
    .select(
        "id",
        F.col("requester_id").alias("requester_id_v"),
        F.col("csat").alias("m_csat"),
    )
    .unpivot(
        ids=["id"],
        values=["requester_id_v", "m_csat"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .orderBy("id", "field")
)
