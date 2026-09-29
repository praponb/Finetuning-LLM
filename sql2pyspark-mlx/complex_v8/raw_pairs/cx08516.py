from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_count_ticket = Window.partitionBy("ticket_channel_count", "ticket_count")
w_agent_hired_on_month_ticket = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "ticket_count").orderBy(F.col("agent_hired_on").desc())
w_all_by_ticket_channel_count = Window.orderBy("ticket_channel_count", "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
    )
)

with_children_events = (
    cleaned_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("assigned", "replied")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

enriched = with_children_events.withColumn("event_type_clean", F.lower(F.trim(F.col("event_type"))))

enriched_agent = (
    enriched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.event_id",
        "s.event_type_clean",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.count(F.when(F.col("channel") == "email", 1)).alias("ticket_channel_count"),
    )
)

with_stats = (
    enriched_agent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_opened_at",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
    )
)

with_trends = (
    with_stats
    .select(
        "*",
        F.size(F.collect_set("agent_tier").over(w_ticket_channel_count_ticket)).alias("distinct_agent_tier_in_ticket_channel_count_tick"),
        F.dense_rank().over(w_agent_hired_on_month_ticket).alias("agent_hired_on_dense_rank"),
        F.first("last_ticket_opened_at", ignorenulls=True).over(w_all_by_ticket_channel_count).alias("first_known_last_ticket_opened_at"),
    )
)

combined_details = (
    with_trends.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .filter(F.col("t.actor").isin("system", "customer"))
    .select(
        "s.id",
        "s.agent_id",
        "s.event_id",
        "s.event_type_clean",
        "s.agent_tier",
        "s.agent_hired_on",
        "s.agent_name",
        "s.ticket_channel_count",
        "s.distinct_agent_tier_in_ticket_channel_count_tick",
        "s.agent_hired_on_dense_rank",
        F.col("t.actor").alias("event_actor"),
        F.col("t.ticket_id").alias("event_ticket_id"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "event_id",
        "agent_tier",
        "agent_hired_on",
        "agent_name",
        "distinct_agent_tier_in_ticket_channel_count_tick",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_tier")), " ", "_")).alias("agent_tier_slug"),
    )
    .orderBy(F.col("agent_name").desc(), "id", "event_id")
    .limit(10)
)
