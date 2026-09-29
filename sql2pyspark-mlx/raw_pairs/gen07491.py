from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "right")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .filter(F.col("stores.city").isin(["Chiang Mai", "Tokyo"]))
    .groupBy(F.col("stores.region"))
    .agg(
        F.round(F.stddev(F.col("orders.shipping_fee")), 2).alias("stddev_shipping_fee"),
    )
    .orderBy(F.col("region").desc())
)
