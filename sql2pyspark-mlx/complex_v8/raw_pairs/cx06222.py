from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_channel").orderBy(F.col("agent_hired_on").desc(), F.col("event_at_ym").desc(), F.col("event_type").desc(), F.col("ticket_id").desc())

filtered = (
    df_ticket_events
    .filter(F.col("actor") != "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

screened = (
    combined_details
    .crossJoin(combined_details.agg(F.avg("ticket_requester_id").alias("overall_avg_ticket_requester_id")))
    .filter(F.col("ticket_requester_id") > F.col("overall_avg_ticket_requester_id"))
    .drop("overall_avg_ticket_requester_id")
)

grouped = (
    screened
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-18").cast("date"))
    .groupBy(
        F.date_format("event_at", "yyyy-MM").alias("event_at_ym"),
        "event_type",
        "ticket_id",
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "reopened", 1).otherwise(0)).alias("n_event_type_reopened"),
        F.count("*").alias("n_rows"),
        F.sum("ticket_requester_id").alias("total_ticket_requester_id"),
    )
)

with_parent = (
    grouped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.event_at_ym",
        "s.event_type",
        "s.ticket_id",
        "s.n_event_type_reopened",
        "s.n_rows",
        "s.total_ticket_requester_id",
        F.col("t.channel").alias("ticket_channel"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

unique_rows = (
    with_parent
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "event_at_ym",
        "n_event_type_reopened",
        "total_ticket_requester_id",
        "ticket_channel",
        "agent_hired_on",
    )
    .orderBy("agent_hired_on")
)
