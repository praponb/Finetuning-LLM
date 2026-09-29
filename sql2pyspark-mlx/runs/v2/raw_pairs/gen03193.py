from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"), "product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
