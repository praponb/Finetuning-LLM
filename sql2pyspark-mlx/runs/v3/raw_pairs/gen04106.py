from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.min(F.col("o.order_date")).alias("first_order_date"),
    )
)
