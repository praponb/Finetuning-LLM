from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .filter(F.col("t.avg_revenue") >= 3000)
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("t.avg_revenue"),
    )
)
