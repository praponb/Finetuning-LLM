from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        F.when(F.col("quantity") >= 261, "high").when(F.col("quantity") >= 119, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .filter(F.col("quantity_range") >= 47)
    .orderBy("quantity_bucket")
)
