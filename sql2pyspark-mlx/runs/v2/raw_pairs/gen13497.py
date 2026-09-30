from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("launch_date").alias("min_launch_date"),
    )
    .filter(F.col("min_cost") >= 220)
    .select("brand", "min_cost", "min_launch_date")
)
