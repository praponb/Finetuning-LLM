from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.revenue_range") >= 46)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("ps.revenue_range"),
        F.col("ps.first_sale_date"),
    )
    .orderBy("supplier_id", F.col("revenue_range").desc(), "id")
    .limit(50)
)
