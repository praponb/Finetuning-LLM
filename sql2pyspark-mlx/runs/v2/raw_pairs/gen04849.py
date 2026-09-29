from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.year("launch_date") >= 2023) & F.array_contains("tags", "bestseller"))
)

agg = (
    base
    .groupBy("brand")
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
)

result = agg.select("brand", "avg_stock_qty", "min_stock_qty")
