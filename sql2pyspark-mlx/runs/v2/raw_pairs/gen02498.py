from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 260, "high").when(F.col("quantity") >= 98, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .filter(F.col("avg_quantity") >= 110)
    .select("quantity_bucket", "store_id", "avg_quantity", "unique_stores")
)
