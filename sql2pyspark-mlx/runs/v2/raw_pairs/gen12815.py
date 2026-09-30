from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("units") >= 12)
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.stddev_revenue") >= 43)
    .select(
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("P.stddev_revenue"),
        F.col("P.units_range"),
    )
)
