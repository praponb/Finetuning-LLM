from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") < "2024-10-12") & F.col("supplier_id").isin([11, 16, 32]) & (F.col("price") <= 640))
)

products_stats = (
    base
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("category") == "Books", F.col("stock_qty")).otherwise(0)).alias("books_stock_qty"),
    )
)

result = products_stats.filter(F.col("books_stock_qty") < 22)
