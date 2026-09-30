from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("order_count"),
        F.max("order_date").alias("latest_order_date"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.segment"),
        F.col("cs.order_count"),
        F.col("cs.latest_order_date"),
    )
)
