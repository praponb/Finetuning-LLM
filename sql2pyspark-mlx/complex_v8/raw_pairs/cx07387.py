from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_agent_id").orderBy(F.col("ticket_closed_at_2").desc(), F.col("id").desc(), F.col("ticket_id").desc())

valid = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 8)
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.date_format(F.col("hired_on"), "yyyy-MM").alias("hired_on_label"),
    )
)

detail_rows_tickets = (
    valid.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.priority") == "P2"), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.team",
        F.col("t.id").alias("ticket_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

combined_details_ticket = (
    detail_rows_tickets.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.month(F.col("t.closed_at")).isin(1, 2))
    .select(
        "s.id",
        "s.agent_name",
        "s.ticket_id",
        F.col("t.closed_at").alias("ticket_closed_at_2"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

deduped = (
    combined_details_ticket
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

combined_details = (
    deduped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.agent_name",
        "s.ticket_id",
        "s.ticket_closed_at_2",
        "s.ticket_agent_id",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "agent_name",
        "ticket_id",
        "ticket_agent_id",
        "ticket_csat",
        F.date_trunc("week", F.col("ticket_closed_at_2")).alias("ticket_closed_at_2_week"),
        F.hour(F.col("ticket_closed_at_2")).alias("ticket_closed_at_2_hour"),
    )
    .orderBy("ticket_csat", F.col("ticket_agent_id").desc())
)
