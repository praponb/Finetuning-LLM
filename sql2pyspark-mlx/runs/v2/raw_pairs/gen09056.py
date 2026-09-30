from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id").isin([1, 2, 3, 8]))
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
