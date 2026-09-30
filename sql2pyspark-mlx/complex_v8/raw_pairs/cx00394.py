from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("ticket_count_3_minus_max").desc(), F.col("id").desc())

filtered_tickets = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.count("*").alias("ticket_count_2"),
    )
)

with_stats = (
    filtered_tickets.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.first_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.sum("requester_id").alias("total_ticket_requester_id"),
        F.count("*").alias("ticket_count_3"),
    )
)

summarized_rows = (
    with_stats.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.total_ticket_requester_id",
        F.coalesce(F.col("t.ticket_count_3"), F.lit(0)).alias("ticket_count_3"),
    )
)

agent_id_stats = (
    summarized_rows
    .groupBy("agent_id")
    .agg(F.max("ticket_count_3").alias("max_ticket_count_3"))
)

compared = (
    summarized_rows.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.ticket_count_3") == F.col("g.max_ticket_count_3"))
    .select(
        "s.*",
        "g.max_ticket_count_3",
        F.round(F.col("s.ticket_count_3") - F.col("g.max_ticket_count_3"), 2).alias("ticket_count_3_minus_max"),
    )
)

unique_rows = (
    compared
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_details_agent = (
    unique_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("H%"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.channel_slug",
        "s.ticket_count",
        "s.first_ticket_closed_at",
        "s.ticket_count_2",
        "s.ticket_count_3",
        "s.max_ticket_count_3",
        "s.ticket_count_3_minus_max",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

result = (
    with_details_agent
    .filter(F.col("ticket_count_3") > 3)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "channel_slug",
        "ticket_count",
        "first_ticket_closed_at",
        "ticket_count_2",
        "max_ticket_count_3",
        "ticket_count_3_minus_max",
        F.length(F.trim(F.col("channel_slug"))).alias("channel_slug_len"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
    )
    .orderBy(F.col("ticket_count_3_minus_max").asc(), "id")
    .limit(10)
)
