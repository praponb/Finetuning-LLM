from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 246, "high").when(F.col("quantity") >= 47, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("last_restock").alias("first_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("quantity_bucket").desc())
)
