from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("last_restock_quarter")
)
