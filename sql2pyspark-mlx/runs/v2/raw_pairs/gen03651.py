from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("ps.units_range"),
        F.col("ps.stddev_revenue"),
    )
    .orderBy(F.col("units_range").desc(), "price")
)
