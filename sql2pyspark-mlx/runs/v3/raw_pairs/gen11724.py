from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.year("last_restock") == 2023) & (F.col("quantity") <= 198))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "product_id",
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("total_quantity") >= 610)
    .orderBy("min_last_restock")
)
