from pyspark.sql import functions as F

base = df_inventory.filter(F.year("last_restock") == 2023)

summary = (
    base
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"), "product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
)

result = (
    summary
    .select("last_restock_quarter", "max_quantity", "min_last_restock")
    .orderBy(F.col("max_quantity").desc(), "last_restock_quarter")
)
