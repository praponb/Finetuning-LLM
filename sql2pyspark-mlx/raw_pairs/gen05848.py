from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.category"),
        F.col("ps.total_units"),
        F.col("ps.stddev_revenue"),
    )
    .orderBy("total_units", F.col("id").desc())
    .limit(3)
)
