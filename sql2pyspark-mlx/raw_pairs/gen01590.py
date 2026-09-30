from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.year("last_restock") == 2023)
    .withColumn("quantity_whole", F.ceil("quantity"))
)

inventory_stats = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 259, "high").when(F.col("quantity") >= 74, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("quantity_whole").alias("max_quantity_whole"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    inventory_stats
    .filter(F.col("avg_quantity") >= 23)
    .select("quantity_bucket", "max_quantity_whole", "avg_quantity")
)
