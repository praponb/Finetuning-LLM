from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 229, "high").when(F.col("quantity") >= 114, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("total_quantity") >= 990)
    .orderBy(F.col("quantity_range").desc(), "product_id", "quantity_bucket")
    .limit(20)
)
