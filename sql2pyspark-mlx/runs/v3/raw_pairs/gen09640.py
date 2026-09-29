from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("avg_quantity") > 56)
)
