from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 7]))
    .groupBy("product_id")
    .agg(
        F.min("units").alias("min_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .filter(F.col("product_summary.stddev_units") >= 39)
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.stock_qty"),
        F.col("product_summary.min_units"),
        F.col("product_summary.stddev_units"),
    )
    .orderBy(F.col("stock_qty").desc())
)
