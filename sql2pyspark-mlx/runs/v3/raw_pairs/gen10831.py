from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        F.when(F.col("quantity") >= 261, "high").when(F.col("quantity") >= 70, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("last_restock").alias("first_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
)
