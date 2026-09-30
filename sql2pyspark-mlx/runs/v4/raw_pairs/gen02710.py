from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("plan").isin(["premium", "basic"]), "Priority").otherwise("Other").alias("plan_group"),
        F.length("plan").alias("plan_length"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("plan_length") >= 80, "HIGH").otherwise("LOW").alias("plan_length_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "ACTIVE").alias("active_events"),
        F.count("*").alias("row_count"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
)
