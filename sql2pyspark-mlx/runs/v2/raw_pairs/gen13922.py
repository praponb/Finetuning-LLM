from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2025-01-10")
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(F.countDistinct("stock_qty").alias("unique_suppliers"))
)
