from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.filter(F.col("sqft").between(4519, 10791)).alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left_semi")
    .filter(F.col("orders.discount") >= 0.27)
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.order_date"),
        F.col("orders.shipping_fee"),
    )
    .orderBy("order_date", "shipping_fee", "id")
    .limit(10)
)
