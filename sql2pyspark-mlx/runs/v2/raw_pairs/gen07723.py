from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.year("launch_date") >= 2025)
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(F.min("price").alias("min_price"))
)
