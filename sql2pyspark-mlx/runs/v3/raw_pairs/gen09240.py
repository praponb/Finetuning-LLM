from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
    .orderBy(F.col("avg_stock_qty").desc())
)
