from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 5]) | F.col("product_id").isin([1, 6, 8]))
)

summary = (
    base
    .groupBy(
        F.year("last_restock").alias("last_restock_year"),
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.min("last_restock").alias("first_last_restock"),
    )
)

result = (
    summary
    .select(
        "last_restock_year",
        "last_restock_quarter",
        "min_last_restock",
        "first_last_restock",
    )
    .orderBy(F.col("first_last_restock").desc(), F.col("last_restock_quarter").desc())
)
