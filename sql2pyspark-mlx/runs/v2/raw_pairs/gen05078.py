from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("o.order_date")).isin([2, 8, 12]))
    .select(F.col("o.id"), F.col("o.amount"), F.col("s.city"))
    .orderBy(F.col("id").desc())
)
