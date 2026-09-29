from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("brand", F.month("launch_date").alias("launch_month"))
    .agg(
        F.max("cost").alias("max_cost"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
    .filter(F.col("avg_stock_qty") > 280)
    .orderBy(F.col("avg_stock_qty").desc(), F.col("launch_month").desc())
)
