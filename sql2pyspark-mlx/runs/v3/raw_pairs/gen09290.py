from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.col("last_restock").between("2023-05-01", "2025-04-01") & F.col("product_id").isin([3, 5, 10]))
    .withColumn("quantity_str", F.col("quantity").cast("string"))
)

summary = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 203, "high").when(F.col("quantity") >= 42, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    summary
    .filter(F.col("avg_quantity") >= 42)
    .select("quantity_bucket", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc())
)
