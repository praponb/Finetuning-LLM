from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.col("agent_id") <= 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        F.array_contains(F.col("tags"), "bug").alias("has_bug"),
    )
)

screened = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier") != "T3") & (F.upper(F.trim(F.col("su.agent_name"))).like("I%")), "left_semi")
)

summary = (
    screened
    .groupBy(F.to_date("opened_at").alias("opened_at_day"), "priority")
    .agg(
        F.sum(F.when(F.col("priority").isin("P3", "P1", "P4"), 1).otherwise(0)).alias("n_priority_in_p3_p1_p4"),
        F.max("opened_at").alias("last_opened_at"),
        F.max("requester_id").alias("max_requester_id"),
        F.sum("requester_id").alias("total_requester_id"),
    )
)

result = (
    summary
    .groupBy("last_opened_at", "n_priority_in_p3_p1_p4")
    .pivot("priority", ["P1", "P2", "P4", "P3"])
    .agg(F.sum("total_requester_id"))
    .orderBy("last_opened_at", "n_priority_in_p3_p1_p4")
)
