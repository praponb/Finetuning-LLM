from pyspark.sql import functions as F

customer_stats = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.min("order_date").alias("min_order_date"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .filter(F.col("customer_stats.total_amount") > 6500)
    .select(
        F.col("customers.is_active"),
        F.col("customers.email"),
        F.col("customer_stats.min_order_date"),
        F.col("customer_stats.total_amount"),
    )
)
