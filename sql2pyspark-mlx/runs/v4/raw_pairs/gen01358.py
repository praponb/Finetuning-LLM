from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(84, 150) | F.col("store_id").isin([2, 3, 4, 5]))
    .rollup(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "store_id",
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.min("last_restock").alias("first_last_restock"),
    )
)
