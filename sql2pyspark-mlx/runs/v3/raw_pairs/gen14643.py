from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2024-08-01")
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 214, "high").when(F.col("quantity") >= 93, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
)
