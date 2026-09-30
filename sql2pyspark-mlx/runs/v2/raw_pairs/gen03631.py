from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "basic", "standard"]))
    .groupBy("customer_id")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.monthly_fee_count") >= 6)
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.segment"),
        F.col("cs.monthly_fee_count"),
    )
    .orderBy(F.col("id").desc(), F.col("email").desc())
)
