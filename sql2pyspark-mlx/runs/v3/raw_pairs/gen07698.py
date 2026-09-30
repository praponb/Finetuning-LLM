from pyspark.sql import functions as F

product_summary = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 5]))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("ps.total_count"),
        F.col("ps.cnt"),
    )
    .orderBy(F.col("cnt").desc(), F.col("id").desc())
    .limit(10)
)
