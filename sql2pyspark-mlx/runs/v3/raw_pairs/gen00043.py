from pyspark.sql import functions as F

result = (
    df_products
    .rollup(
        "brand",
        F.when(F.col("stock_qty") >= 424, "high").when(F.col("stock_qty") >= 83, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
    .orderBy(F.col("brand").desc())
)
