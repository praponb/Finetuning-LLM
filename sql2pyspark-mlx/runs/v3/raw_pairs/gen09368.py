from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]) & (F.col("store_id").isin([1, 2, 3, 6]) | F.col("store_id").isin([1, 4])))
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 247, "high").when(F.col("quantity") >= 60, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy("quantity_bucket")
)
