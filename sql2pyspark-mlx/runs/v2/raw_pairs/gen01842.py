from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2023-11-01")
    .groupBy("category", "brand")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .filter(F.col("avg_price") > 26)
)
