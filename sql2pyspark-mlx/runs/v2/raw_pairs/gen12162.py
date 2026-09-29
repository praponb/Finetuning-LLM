from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
    )
)
