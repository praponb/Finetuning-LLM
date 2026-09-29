from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .groupBy(F.col("stores.id"), F.col("stores.city"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.sum(F.when(F.col("orders.channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
)
