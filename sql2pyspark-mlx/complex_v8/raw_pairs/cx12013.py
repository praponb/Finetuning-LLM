from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 24)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

per_group = (
    recent
    .groupBy("event_type_or_unknown", "actor_group")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(F.col("actor").isin("agent", "customer"), 1)) / F.count("*"), 1).alias("pct_actor_in_agent_customer"),
    )
)

qualified = (
    per_group
    .crossJoin(per_group.agg(F.min("distinct_ids").alias("overall_min_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_min_distinct_ids"))
    .drop("overall_min_distinct_ids")
)

result = (
    qualified
    .filter(F.col("actor_group") == "group_b")
    .select(
        "event_type_or_unknown",
        "actor_group",
        "distinct_ids",
        "pct_actor_in_agent_customer",
    )
    .orderBy("actor_group", F.col("distinct_ids").desc())
)
