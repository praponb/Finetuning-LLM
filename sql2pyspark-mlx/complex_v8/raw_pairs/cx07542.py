from pyspark.sql import functions as F

valid = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.array_contains(F.col("tags"), "upgrade")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "tags",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
        F.when(F.col("requester_id") >= 18, "large").when(F.col("requester_id") >= 8, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

without_matches = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").isNotNull()) & (F.lower(F.col("su.agent_name")).like("%sa%")), "left_semi")
)

rollup_base = (
    without_matches
    .groupBy("requester_id_tier", "agent_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("opened_at").alias("last_opened_at"),
        F.percentile_approx("requester_id", 0.5).alias("median_requester_id"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    rollup_base
    .select(
        "requester_id_tier",
        "agent_id",
        "distinct_ids",
        "last_opened_at",
        "median_requester_id",
        "distinct_agent_ids",
        F.round(F.col("median_requester_id") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("median_requester_id_per_distinct_ids"),
    )
    .orderBy("last_opened_at")
)
