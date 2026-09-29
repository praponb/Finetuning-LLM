from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3]) & ((F.col("quantity") <= 82) | (F.col("product_id") == 1)))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.sum("quantity").alias("total_quantity"),
    )
)
