from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2025-02-01") & (F.col("last_restock").between("2023-03-01", "2025-04-01") | F.col("store_id").isin([1, 4, 5])))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy(F.col("last_restock_cohort").desc())
)
