from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2025-08-05") & F.col("product_id").isin([3, 4, 7, 10]) & (F.col("quantity") >= 113))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") < 120)
    .orderBy(F.col("min_quantity").desc())
)
