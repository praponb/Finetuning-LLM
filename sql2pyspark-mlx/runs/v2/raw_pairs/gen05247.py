from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .groupBy(F.col("s.region"), F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
    .orderBy("region", "opened_year")
    .limit(5)
)
