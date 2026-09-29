from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.size("tags") > 1) & F.array_contains("tags", "sale") & F.col("launch_date").between("2024-06-01", "2025-10-16"))
)

products_stats = (
    recent_products
    .groupBy("category")
    .agg(
        F.countDistinct("cost").alias("unique_categorys"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)

result = (
    products_stats
    .select("category", "unique_categorys", "avg_stock_qty")
    .orderBy(F.col("unique_categorys").desc())
)
