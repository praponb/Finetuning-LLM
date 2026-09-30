from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "right")
    .filter(F.col("orders.discount").between(0.11, 0.39))
    .groupBy(
        F.when(F.col("stores.sqft") >= 14371, "high").when(F.col("stores.sqft") >= 3464, "mid").otherwise("low").alias("sqft_bucket"),
        F.col("stores.region"),
    )
    .agg(
        (F.max(F.col("orders.shipping_fee")) - F.min(F.col("orders.shipping_fee"))).alias("shipping_fee_range"),
        F.min(F.col("orders.order_date")).alias("first_order_date"),
    )
    .orderBy(F.col("shipping_fee_range").desc())
)
