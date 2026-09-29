from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team").orderBy("hired_on", "id")

prepared = (
    df_support_agents
    .filter((F.length(F.trim(F.col("agent_name"))) >= 4) & (F.col("hired_on").isNotNull()))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
    )
)

deduped = (
    prepared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

detail_rows_tickets = (
    deduped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.team",
        F.col("t.id").alias("ticket_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

annotated = (
    detail_rows_tickets
    .filter(F.lower(F.col("agent_name")).like("%or%"))
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("ticket_closed_at")).alias("days_since_ticket_closed_at"),
    )
)

enriched = (
    annotated
    .filter(F.col("team") != "Onboarding")
    .select(
        "*",
        F.concat_ws("|", F.col("agent_name"), F.col("team")).alias("agent_name_team_label"),
        F.substring(F.trim(F.col("agent_name")), 1, 4).alias("agent_name_prefix"),
    )
)

result = (
    enriched
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "agent_name",
        "team",
        "ticket_id",
        "ticket_csat",
        "days_since_ticket_closed_at",
        "agent_name_team_label",
        "agent_name_prefix",
    )
    .orderBy(F.col("id").asc())
)
