from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("monthly_fee").isNull())
    .withColumn("plan_status_key", F.concat_ws("-", "plan", "status"))
    .withColumn("plan_length", F.length("plan"))
)

result = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .cube(F.col("p.city").alias("city"), F.col("p.country").alias("country"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("src.plan_length")), 2).alias("avg_plan_length"),
        F.min(F.col("src.monthly_fee")).alias("min_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
)
