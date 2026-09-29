from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(139, 145) | F.col("store_id").isin([2, 3]))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .orderBy("last_restock_cohort")
)
