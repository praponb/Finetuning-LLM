from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "brand",
        F.quarter("launch_date").alias("launch_quarter"),
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)
