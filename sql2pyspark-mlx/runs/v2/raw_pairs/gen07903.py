from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("revenue").alias("max_revenue"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
        F.col("ps.max_revenue"),
    )
    .orderBy("supplier_id")
)
