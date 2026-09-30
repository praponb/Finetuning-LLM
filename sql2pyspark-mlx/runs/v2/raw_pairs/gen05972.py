from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "right")
    .filter(F.col("orders.channel").isin(["web", "partner"]))
    .select(
        F.col("orders.store_id"),
        F.col("stores.region"),
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
    )
    .orderBy("region", "store_id")
)
