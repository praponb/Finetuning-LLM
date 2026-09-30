from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("ps.max_revenue"),
        F.col("ps.units_range"),
    )
)
