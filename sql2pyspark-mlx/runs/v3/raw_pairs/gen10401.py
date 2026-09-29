from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("product_id").isin([2, 6]) | F.month("last_restock").isin([2, 6, 9])))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        F.month("last_restock").alias("last_restock_month"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") < 190)
    .drop("max_quantity")
    .orderBy(F.col("quantity_count").desc())
)
