from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.status") == "RETURNED")
    .groupBy(F.col("customers.is_active"))
    .agg(
        F.avg(F.col("orders.discount")).alias("avg_discount"),
        F.count(F.col("orders.shipping_fee")).alias("shipping_fee_count"),
    )
    .orderBy("avg_discount")
)
