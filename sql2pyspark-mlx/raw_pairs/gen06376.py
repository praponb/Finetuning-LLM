from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .filter(F.col("avg_cost") > 550)
    .select("brand", "avg_cost", "latest_launch_date")
    .orderBy("avg_cost")
)
