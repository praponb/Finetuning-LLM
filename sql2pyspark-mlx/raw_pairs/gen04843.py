from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"))
    .orderBy("opened_month", F.col("avg_quantity").desc())
    .limit(50)
)
