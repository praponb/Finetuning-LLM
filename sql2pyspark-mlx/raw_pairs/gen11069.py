from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 10)
    .select("store_id", "min_last_restock", "cnt")
)
