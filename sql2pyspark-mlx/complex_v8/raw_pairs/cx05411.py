from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        F.concat_ws("-", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
)

with_details = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

stats = (
    with_details
    .filter(F.col("requester_id").between(4, 18))
    .groupBy(
        F.date_trunc("month", "closed_at").alias("closed_at_month"),
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("agent_tier")).alias("agent_tier_list"),
    )
)

tagged = (
    stats
    .select(
        "closed_at_month",
        "opened_at_month",
        "row_count",
        "agent_tier_list",
        F.when(F.col("row_count") >= 9, "A").when(F.col("row_count") >= 5, "B").otherwise("C").alias("row_count_tier"),
        F.year(F.col("closed_at_month")).alias("closed_at_month_year"),
    )
)

result = (
    tagged
    .select(
        "closed_at_month",
        "opened_at_month",
        "row_count",
        "closed_at_month_year",
    )
    .unpivot(
        ids=["closed_at_month", "opened_at_month"],
        values=["row_count", "closed_at_month_year"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .orderBy("closed_at_month", "opened_at_month", "field")
)
