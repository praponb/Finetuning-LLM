from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.length("product_name") >= 11) & (F.col("category") == "Sports"))
)

summary = (
    base
    .groupBy("category", "brand")
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
    )
)

result = (
    summary
    .filter(F.col("stddev_stock_qty") >= 45)
    .select("category", "brand", "max_stock_qty")
    .orderBy("category")
)
