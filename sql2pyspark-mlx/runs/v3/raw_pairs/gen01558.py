from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5, 8, 10]))
    .groupBy(
        F.month("last_restock").alias("last_restock_month"),
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("unique_stores") > 4)
    .select(
        "last_restock_month",
        "last_restock_cohort",
        "unique_stores",
        "min_quantity",
    )
)
