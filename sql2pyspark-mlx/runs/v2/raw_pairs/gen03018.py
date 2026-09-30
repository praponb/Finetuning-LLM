from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2024-03-05", "2025-01-10") | (F.col("cost") < 470))
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)
