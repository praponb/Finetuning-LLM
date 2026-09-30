from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") < 96) & F.col("last_restock").between("2023-06-24", "2025-08-01") & (F.col("quantity") <= 115))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("avg_quantity") > 150)
)
