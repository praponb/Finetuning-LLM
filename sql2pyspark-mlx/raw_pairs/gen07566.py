from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.total_monthly_fee") > 1200)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.age"),
        F.col("cs.unique_plans"),
        F.col("cs.total_monthly_fee"),
    )
)
