from pyspark.sql import functions as F

valid_agents = (
    df_support_agents
    .filter((F.year(F.col("hired_on")) == 2024) & (F.col("team") == "Billing"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.quarter(F.col("hired_on")).alias("hired_on_quarter"),
        F.date_format(F.col("hired_on"), "MMM yyyy").alias("hired_on_label"),
    )
)

exploded_detail_tickets = (
    valid_agents.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.hired_on_quarter",
        "s.hired_on_label",
        F.col("t.id").alias("ticket_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

derived = (
    exploded_detail_tickets
    .select(
        "id",
        "hired_on_label",
        "ticket_id",
        "ticket_tags",
        "ticket_channel",
        F.round(F.col("hired_on_quarter") / F.nullif(F.col("ticket_csat"), F.lit(0)), 3).alias("hired_on_quarter_per_ticket_csat"),
        F.coalesce(F.col("ticket_csat"), F.lit(0)).alias("ticket_csat_filled"),
        F.coalesce(F.col("ticket_csat"), F.lit(0)).alias("ticket_csat_filled_2"),
    )
)

joined = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.hired_on_label",
        "s.ticket_id",
        "s.ticket_tags",
        "s.ticket_channel",
        "s.ticket_csat_filled",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined
    .groupingSets(
        [["ticket_agent_id", "ticket_csat_filled"], ["ticket_agent_id"], []],
        "ticket_agent_id", "ticket_csat_filled",
    )
    .agg(
        F.grouping("ticket_agent_id").alias("is_total_ticket_agent_id"),
        F.countDistinct("ticket_channel").alias("distinct_ticket_channels"),
    )
)
