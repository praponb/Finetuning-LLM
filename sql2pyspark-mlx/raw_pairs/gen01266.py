from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") > 126) & (F.col("price") <= 540))
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        "category",
        "brand",
    )
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("stddev_stock_qty") < 59)
    .orderBy(F.col("stddev_stock_qty").desc())
)
