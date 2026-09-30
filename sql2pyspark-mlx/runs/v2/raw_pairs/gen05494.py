from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(500, 580))
    .groupBy(
        F.when(F.col("stock_qty") >= 393, "high").when(F.col("stock_qty") >= 173, "mid").otherwise("low").alias("stock_qty_bucket"),
        "brand",
        "category",
    )
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.countDistinct("price").alias("unique_suppliers"),
    )
)
