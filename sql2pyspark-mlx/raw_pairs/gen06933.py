from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"), F.col("s.region"))
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 120)
    .orderBy("region")
)
