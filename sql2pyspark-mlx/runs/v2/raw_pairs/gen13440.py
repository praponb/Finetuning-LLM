from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.avg("price").alias("avg_price"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .orderBy(F.col("avg_price").desc(), "category")
)
