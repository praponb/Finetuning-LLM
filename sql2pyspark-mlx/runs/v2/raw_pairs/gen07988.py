from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.store_id"))
    .agg(
        F.min(F.col("s.shipped_date")).alias("min_shipped_date"),
        F.countDistinct(F.col("s.cost")).alias("unique_orders"),
    )
    .orderBy("min_shipped_date", F.col("store_id").desc())
    .limit(50)
)
