from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2025-05-01")
    .groupBy(
        "product_id",
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("inventory_count"),
    )
    .filter(F.col("inventory_count") < 5)
    .drop("inventory_count")
)
