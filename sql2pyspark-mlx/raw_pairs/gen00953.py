from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-03-11", "2025-03-24"))
    .groupBy("customer_id")
    .agg(F.countDistinct("monthly_fee").alias("unique_customers"))
    .orderBy(F.col("unique_customers").desc(), "customer_id")
    .limit(1)
)
