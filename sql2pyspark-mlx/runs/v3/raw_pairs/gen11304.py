from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.year("end_date") == 2024)
    .groupBy("customer_id")
    .agg(F.countDistinct("monthly_fee").alias("unique_plans"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.unique_plans") > 7)
    .select(F.col("c.id"), F.col("c.city"), F.col("cs.unique_plans"))
    .orderBy("city", "id")
    .limit(5)
)
