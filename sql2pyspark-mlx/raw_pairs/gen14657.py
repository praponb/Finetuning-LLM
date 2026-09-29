from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", F.quarter("launch_date").alias("launch_quarter"), "supplier_id")
    .agg(F.min("price").alias("min_price"))
    .filter(F.col("min_price") < 900)
)
