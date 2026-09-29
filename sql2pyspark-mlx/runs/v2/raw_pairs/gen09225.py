from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.stddev_revenue") > 64)
    .select(F.col("p.price"), F.col("ps.min_sale_date"), F.col("ps.stddev_revenue"))
)
