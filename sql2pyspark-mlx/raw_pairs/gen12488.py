from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("store_name").isNotNull()).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("o.discount"),
    )
)
