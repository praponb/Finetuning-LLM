from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.max("end_date").alias("last_end_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "left")
    .select(F.col("c.is_active"), F.col("C.last_end_date"))
    .orderBy("last_end_date")
)
