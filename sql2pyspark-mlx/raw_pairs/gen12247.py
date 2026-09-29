from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.filter(F.col("city").isin(["Singapore", "Bangkok", "Chiang Mai"])).alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left_semi")
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("orders.store_id"),
        F.col("orders.channel"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(3)
)
