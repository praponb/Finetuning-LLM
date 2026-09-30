from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_priority").orderBy("days_since_ticket_opened_at", "id", "ticket_tag")

base = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_len",
        "s.event_type_ref",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

combined_details_2 = (
    combined_details.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_len",
        "s.event_type_ref",
        "s.ticket_tags",
        "s.ticket_channel",
        "s.ticket_priority",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

tagged = (
    combined_details_2
    .filter(F.col("ticket_channel") == "phone")
    .select(
        "*",
        F.concat(F.col("event_type_ref"), F.lit("#"), F.col("ticket_agent_id").cast("string")).alias("event_type_ref_ref"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("ticket_opened_at")).alias("days_since_ticket_opened_at"),
    )
)

exploded = (
    tagged
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_len",
        "event_type_ref",
        "ticket_channel",
        "ticket_priority",
        "ticket_opened_at",
        "ticket_agent_id",
        "agent_tier",
        "agent_name",
        "event_type_ref_ref",
        "days_since_ticket_opened_at",
        F.explode_outer("ticket_tags").alias("ticket_tag"),
    )
)

latest_per_key = (
    exploded
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_activity = (
    latest_per_key
    .crossJoin(latest_per_key.agg(F.max("days_since_ticket_opened_at").alias("overall_max_days_since_ticket_opened_at")))
    .filter(F.col("days_since_ticket_opened_at") < F.col("overall_max_days_since_ticket_opened_at"))
    .drop("overall_max_days_since_ticket_opened_at")
)

result = (
    with_activity
    .groupBy("event_type_len", "agent_tier")
    .pivot("ticket_channel", ["web", "phone", "email", "chat"])
    .agg(F.avg("days_since_ticket_opened_at").alias("avg_v"), F.sum("days_since_ticket_opened_at").alias("sum_v"))
    .orderBy("event_type_len", "agent_tier")
)
