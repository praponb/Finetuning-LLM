from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.max_revenue") > 3300)
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("t.min_revenue"),
        F.col("t.max_revenue"),
    )
)
