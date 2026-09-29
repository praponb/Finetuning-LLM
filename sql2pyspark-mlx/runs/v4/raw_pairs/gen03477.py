from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("cost").between(260, 620) & F.col("stock_qty").isNull()) | (F.col("launch_date") > "2024-08-04"))
)

category_summary = (
    recent_products
    .groupBy("category")
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
)

result = (
    category_summary
    .select("category", "avg_stock_qty", "electronics_count")
    .orderBy("category", F.col("electronics_count").desc())
)
