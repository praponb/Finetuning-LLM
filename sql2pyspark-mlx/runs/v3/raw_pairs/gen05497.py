from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("avg_quantity") > 220)
    .orderBy(F.col("last_restock_cohort").desc())
)
