from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 2) & F.col("product_id").isin([1, 10]))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("last_restock_cohort").desc())
)
