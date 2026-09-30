from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.min("price").alias("min_price"),
    )
    .filter(F.col("min_price") > 380)
)
