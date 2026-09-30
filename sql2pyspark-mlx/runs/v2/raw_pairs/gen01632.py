from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2025-06-13", "2025-10-01") & ((F.col("last_restock") <= "2025-08-01") | (F.col("quantity") <= 109)))
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 192, "high").when(F.col("quantity") >= 115, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("quantity").alias("quantity_count"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("quantity_count") < 1)
    .orderBy(F.col("stddev_quantity").desc())
)
