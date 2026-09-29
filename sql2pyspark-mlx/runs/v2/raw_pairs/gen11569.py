from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .groupBy("brand")
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("launch_date").alias("min_launch_date"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("min_cost") >= 560)
)
