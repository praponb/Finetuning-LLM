from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("channel").orderBy("closed_at", "id")

filtered = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("csat") < 4))
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
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

deduped = (
    with_parent
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

profiled = (
    deduped.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.last_ticket_opened_at", "t.last_ticket_closed_at")
)

result = (
    profiled
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
        "closed_at_label",
        "opened_at_label",
        "agent_tier",
        "agent_team",
        "last_ticket_opened_at",
        F.to_date(F.col("last_ticket_opened_at")).alias("last_ticket_opened_at_date"),
    )
    .orderBy(F.col("agent_id").desc())
)
