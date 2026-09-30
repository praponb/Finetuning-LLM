from pyspark.sql import functions as F

base = df_products.filter((F.size("tags") > 1) & (F.col("cost") <= 190))

category_summary = (
    base
    .groupBy("category")
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.avg("price").alias("avg_price"),
    )
)

result = (
    category_summary
    .filter(F.col("stddev_stock_qty") >= 59)
    .orderBy("stddev_stock_qty", "category")
    .limit(10)
)
