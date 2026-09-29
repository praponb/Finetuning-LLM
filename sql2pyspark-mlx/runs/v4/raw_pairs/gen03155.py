from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.max("cost").alias("max_cost"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .filter(F.col("max_cost") >= 310)
)
