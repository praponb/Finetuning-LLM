from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.count("price").alias("price_count"),
    )
    .filter(F.col("price_count") > 5)
    .drop("price_count")
)
