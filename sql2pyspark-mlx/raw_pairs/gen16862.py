from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.region"),
    )
)
