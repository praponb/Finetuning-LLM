from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2023-04-01")
    .withColumn("is_new", F.array_contains("tags", "new"))
)
