from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 170) & (F.col("quantity") == 181))
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "product_id",
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("last_restock").alias("last_last_restock"),
    )
)
