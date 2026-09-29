from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("cost").alias("cost_count"),
    )
)
