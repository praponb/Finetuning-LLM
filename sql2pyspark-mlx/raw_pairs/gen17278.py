from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Desk%"))
    .groupBy("category", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.countDistinct("cost").alias("unique_brands"),
    )
    .filter(F.col("stddev_stock_qty") > 24)
    .orderBy(F.col("category").desc())
)
