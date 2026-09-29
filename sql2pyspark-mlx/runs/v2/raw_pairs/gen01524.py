from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-01-01", "2024-07-01"))
    .groupBy("product_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
