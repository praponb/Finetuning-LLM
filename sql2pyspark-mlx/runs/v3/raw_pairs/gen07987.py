from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("orders.discount") >= 0.2)
    .groupBy(F.col("stores.region"))
    .agg(
        F.count(F.col("orders.shipping_fee")).alias("shipping_fee_count"),
        F.round(((100 * F.sum(F.when(F.col("orders.status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
    .orderBy(F.col("shipping_fee_count").desc())
)
