from pyspark.sql import functions as F

recent_products = df_products.filter(F.col("launch_date").between("2023-06-01", "2025-05-01"))

agg = (
    recent_products
    .groupBy("category")
    .agg(
        F.round(F.stddev("stock_qty"), 2).alias("stddev_stock_qty"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
    )
)

result = (
    agg
    .filter(F.col("stark_count") >= 7)
    .select("category", "stark_count")
    .orderBy("category")
)
