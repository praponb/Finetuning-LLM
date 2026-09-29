from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.store_id").isin([2, 4, 5, 6]))
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
    .orderBy(F.col("opened_month").desc(), F.col("min_quantity").desc())
)
