from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.brand"),
        F.col("ps.cnt"),
        F.col("ps.latest_sale_date"),
    )
    .orderBy("id")
)
