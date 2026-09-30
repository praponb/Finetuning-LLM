from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(1, 3)) & (F.array_contains(F.col("tags"), "upgrade")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
    )
)

flattened = (
    valid
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "days_since_opened_at",
        "closed_at_quarter",
        F.explode_outer("tags").alias("tag"),
    )
)

qualified = (
    flattened.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "customer") & (F.col("t.event_type") == "assigned"), "left_anti")
    .filter(F.col("days_since_opened_at") < 595)
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
        F.max("requester_id").alias("max_ticket_requester_id"),
    )
)

with_counts = (
    qualified.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
        "t.max_ticket_requester_id",
    )
)

tagged = (
    with_counts
    .select(
        "*",
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
        (F.col("ticket_count") + F.col("days_since_opened_at")).alias("ticket_count_plus_days_since_opened_at"),
        F.when(F.col("tag").isin("login", "upgrade"), "group_a").when(F.col("tag") == "outage", "group_b").otherwise("other").alias("tag_group"),
    )
)

tagged_2 = (
    tagged
    .select(
        "*",
        F.concat_ws("-", F.col("tag"), F.col("channel")).alias("tag_channel_label"),
        F.round(F.col("ticket_count_2") / F.nullif(F.col("days_since_opened_at"), F.lit(0)), 3).alias("ticket_count_2_per_days_since_opened_at"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
)

result = (
    tagged_2
    .select(
        "id",
        "tag",
        F.col("ticket_count_plus_days_since_opened_at").alias("ticket_count_plus_days_since_opened_at_v"),
        F.col("closed_at_quarter").alias("m_closed_at_quarter"),
        F.col("ticket_count_2").alias("ticket_count_2_v"),
    )
    .unpivot(
        ids=["id", "tag"],
        values=["ticket_count_plus_days_since_opened_at_v", "m_closed_at_quarter", "ticket_count_2_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
