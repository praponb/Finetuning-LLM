from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 211, "high").when(F.col("quantity") >= 34, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 58)
    .select("quantity_bucket", "latest_last_restock", "avg_quantity")
)
