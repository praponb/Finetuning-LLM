from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "right")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .groupBy(F.col("customers.is_active"), F.col("customers.country"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("orders.channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
        F.count(F.when(F.col("orders.status") == "SHIPPED", 1)).alias("shipped_count"),
    )
)
