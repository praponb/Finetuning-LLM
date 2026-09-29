from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Home", "Grocery"]))
    .groupBy("brand")
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.min("launch_date").alias("first_launch_date"),
    )
)
