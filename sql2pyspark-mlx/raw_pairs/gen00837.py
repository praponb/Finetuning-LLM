from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("stock_qty") >= 345, "high").when(F.col("stock_qty") >= 78, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.countDistinct("cost").alias("unique_categorys"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
    .orderBy(F.col("stddev_stock_qty").desc(), "stock_qty_bucket")
    .limit(1)
)
