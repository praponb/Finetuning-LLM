from pyspark.sql import functions as F

result = (
    df_products
    .filter(~((F.col("cost") >= 300) & F.col("price").between(970, 1130)))
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.min("launch_date").alias("first_launch_date"),
    )
)
