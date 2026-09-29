from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(F.col("o.store_id"))
    .agg(
        F.count(F.when(F.col("s.carrier") == "UPS", 1)).alias("ups_count"),
        F.sum(F.when(F.col("s.carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
    )
)
