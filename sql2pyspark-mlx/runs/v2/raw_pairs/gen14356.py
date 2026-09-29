from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.lower(F.trim("store_name")) == "siam flagship").alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("o.store_id"),
    )
    .orderBy("order_date", F.col("store_id").desc(), "id")
    .limit(10)
)
