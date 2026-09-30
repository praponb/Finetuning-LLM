from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 5]))
    .groupBy("product_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.avg_revenue") >= 62)
    .select(F.col("p.launch_date"), F.col("t.avg_revenue"))
)
