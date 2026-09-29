from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("launch_date").between("2023-06-01", "2024-07-17"))

summary = (
    recent_products
    .groupBy("category")
    .agg(
        F.count("price").alias("price_count"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    summary
    .filter(F.col("price_count") < 7)
    .select("category", "price_count", "total_stock_qty")
    .orderBy(F.col("total_stock_qty").desc(), "category")
    .limit(50)
)
