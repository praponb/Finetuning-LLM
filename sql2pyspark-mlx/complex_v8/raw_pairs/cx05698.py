from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

summary = (
    valid
    .groupBy(F.year("event_at").alias("event_at_year"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("actor").isin("system", "agent"), 1)) / F.count("*"), 1).alias("pct_actor_in_system_agent"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("event_type") == "resolved", 1)).alias("n_event_type_resolved"),
    )
)

screened = (
    summary
    .crossJoin(summary.agg(F.avg("event_at_year").alias("overall_avg_event_at_year")))
    .filter(F.col("event_at_year") > F.col("overall_avg_event_at_year"))
    .drop("overall_avg_event_at_year")
)

labeled = (
    screened
    .withColumn("distinct_ids_tier", F.when(F.col("distinct_ids") >= 9, "HIGH").when(F.col("distinct_ids") >= 4, "MEDIUM").otherwise("LOW"))
)

result = (
    labeled
    .select(
        "event_at_year",
        "cnt",
        "pct_actor_in_system_agent",
        "distinct_ids",
        "n_event_type_resolved",
        F.lower(F.regexp_replace(F.trim(F.col("distinct_ids_tier")), " ", "_")).alias("distinct_ids_tier_slug"),
    )
    .orderBy(F.col("n_event_type_resolved").asc_nulls_last())
)
