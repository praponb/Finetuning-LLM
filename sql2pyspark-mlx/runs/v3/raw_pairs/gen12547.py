from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2025-02-01")
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 500)
    .orderBy(F.col("last_restock_cohort").desc())
)
