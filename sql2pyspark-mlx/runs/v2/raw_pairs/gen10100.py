from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("last_restock").alias("min_last_restock"),
    )
)
