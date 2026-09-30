from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.total_units") > 220)
    .select(
        F.col("products.category"),
        F.col("products.stock_qty"),
        F.col("t.total_units"),
        F.col("t.latest_sale_date"),
    )
    .orderBy(F.col("stock_qty").desc(), "latest_sale_date")
)
