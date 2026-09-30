from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        F.quarter("launch_date").alias("launch_quarter"),
    )
    .agg(F.min("launch_date").alias("min_launch_date"))
    .orderBy("launch_month")
)
