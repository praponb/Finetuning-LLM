from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .groupBy(
        F.month("last_restock").alias("last_restock_month"),
        "store_id",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
)
