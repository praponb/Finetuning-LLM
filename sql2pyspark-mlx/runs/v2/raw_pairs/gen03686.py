from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.max("last_restock").alias("latest_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") < 170)
    .orderBy(
        F.col("min_quantity").desc(),
        F.col("last_restock_quarter").desc(),
        "store_id",
    )
    .limit(50)
)
