from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2024-02-11").cast("date")) & F.col("customer_id").isNotNull() & F.col("start_date").between("2023-11-25", "2025-08-22"))
    .select("*", F.trim("status").alias("clean_status"))
)

result = (
    base
    .groupBy(
        F.when(F.col("monthly_fee") >= 219, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_rows"),
    )
)
