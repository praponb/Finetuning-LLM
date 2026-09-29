from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull() & (F.col("store_id") == 2))
    .groupBy(
        "store_id",
        "product_id",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
