from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(F.min(F.col("o.order_date")).alias("first_order_date"))
    .orderBy(F.col("first_order_date").desc())
)
