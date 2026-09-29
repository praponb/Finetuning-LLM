from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date", "brand", F.array_contains("tags", "sale").alias("is_sale"))
    .orderBy(F.col("brand").desc())
)
