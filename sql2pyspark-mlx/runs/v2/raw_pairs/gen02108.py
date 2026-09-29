from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .filter(F.col("avg_quantity") >= 210)
    .orderBy("city", F.col("avg_quantity").desc(), "region")
    .limit(3)
)
