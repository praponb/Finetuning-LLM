from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.total_units") >= 200)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("ps.total_units"),
        F.col("ps.avg_revenue"),
    )
)
