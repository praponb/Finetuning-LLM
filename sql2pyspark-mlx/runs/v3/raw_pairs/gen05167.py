from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .select(
        F.col("s.cost"),
        F.col("o.store_id"),
        F.when(F.col("s.status") == "LOST", 1).otherwise(0).alias("is_lost"),
    )
)
