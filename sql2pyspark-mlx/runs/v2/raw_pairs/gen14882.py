from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "brand",
        F.when(F.col("stock_qty") >= 375, "high").when(F.col("stock_qty") >= 51, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.countDistinct("cost").alias("unique_brands"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
    .orderBy(F.col("unique_brands").desc(), "brand", "stock_qty_bucket")
    .limit(5)
)
