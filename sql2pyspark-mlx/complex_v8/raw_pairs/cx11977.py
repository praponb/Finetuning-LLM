from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")

filtered = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

combined_details_ticket = (
    deduped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.agent_id") <= 5)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        "s.event_type_len",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    combined_details_ticket
    .groupingSets(
        [["actor", "agent_tier", "event_type"], ["actor"], ["agent_tier"], []],
        "actor", "agent_tier", "event_type",
    )
    .agg(
        F.round(F.stddev("days_since_event_at"), 2).alias("stddev_days_since_event_at"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("event_type_len"), 2).alias("stddev_event_type_len"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "stddev_days_since_event_at",
        "n_rows",
        "stddev_event_type_len",
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
