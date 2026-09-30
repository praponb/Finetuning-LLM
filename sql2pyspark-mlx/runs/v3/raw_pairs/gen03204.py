from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3]))
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 210, "high").when(F.col("quantity") >= 39, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_stores"))
)
