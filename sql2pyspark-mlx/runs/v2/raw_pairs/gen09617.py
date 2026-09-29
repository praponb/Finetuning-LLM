from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.min("price").alias("min_price"),
    )
    .orderBy("brand")
)
