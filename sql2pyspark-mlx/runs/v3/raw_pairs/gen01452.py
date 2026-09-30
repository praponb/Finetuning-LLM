from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("city") == "Singapore").alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
    )
    .orderBy("customer_id", "id")
    .limit(10)
)
