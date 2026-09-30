from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft") != 16003)
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.max(F.col("o.order_date")).alias("latest_order_date"),
    )
)
