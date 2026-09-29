from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-07-18", "2025-09-01"))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("last_restock").alias("min_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("quantity_range").desc())
)
