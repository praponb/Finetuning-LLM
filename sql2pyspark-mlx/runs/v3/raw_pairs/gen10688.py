from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .filter(F.col("discount") < 0.25)
    .groupBy("customer_id")
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.discount_count") >= 6)
    .select(F.col("c.age"), F.col("t.discount_count"))
)
