from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.month("sale_date").isin([3, 8, 11]))
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("ps.latest_sale_date"),
        F.col("ps.max_units"),
    )
)
