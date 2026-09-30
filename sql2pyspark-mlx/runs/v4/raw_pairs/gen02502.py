from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 9) & F.col("last_restock").between("2023-05-01", "2023-09-27") & F.col("store_id").isin([1, 3, 5]))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("max_quantity") < 220)
    .orderBy(F.col("unique_products").desc(), "last_restock_cohort")
)
