from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("revenue").alias("max_revenue"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .filter(F.col("P.max_revenue") >= 1900)
    .select(F.col("p.product_name"), F.col("p.cost"), F.col("P.max_revenue"))
    .orderBy(F.col("max_revenue").desc())
)
