from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-07-01", "2024-09-01"))
    .groupBy("customer_id")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.credit_limit"),
        F.col("cs.unique_customers"),
        F.col("cs.active_count"),
    )
)
