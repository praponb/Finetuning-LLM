from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0)).alias("premium_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("cs.unique_customers"),
        F.col("cs.premium_count"),
    )
    .orderBy("country", "unique_customers", "id")
    .limit(5)
)
