from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Initech", "Umbrella"]))
    .groupBy("category")
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.min("launch_date").alias("first_launch_date"),
    )
    .filter(F.col("unique_brands") < 4)
    .select("category", "unique_brands", "first_launch_date")
)
