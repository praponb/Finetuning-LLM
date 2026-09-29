from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        F.when(F.col("price") >= 1010, "high").when(F.col("price") >= 220, "mid").otherwise("low").alias("price_bucket"),
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
    )
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.count("*").alias("cnt"),
        F.countDistinct("price").alias("unique_brands"),
    )
    .orderBy("category")
)
