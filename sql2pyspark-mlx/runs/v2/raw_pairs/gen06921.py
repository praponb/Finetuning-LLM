from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("quantity").between(76, 236) & (F.col("store_id").isin([2, 5]) | (F.col("quantity") >= 148)))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

store_id_summary = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)

result = (
    store_id_summary
    .filter(F.col("unique_stores") >= 8)
    .select("store_id", "unique_stores")
    .orderBy(F.col("unique_stores").desc())
)
