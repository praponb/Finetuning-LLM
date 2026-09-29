from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.year(F.col("o.order_date")) == 2025)
    .select(F.col("o.id"), F.col("o.status"), F.col("o.store_id"), F.col("s.city"))
)
