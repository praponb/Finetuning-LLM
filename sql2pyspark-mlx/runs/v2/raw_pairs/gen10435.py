from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"), "category")
    .agg(F.min("launch_date").alias("first_launch_date"))
)
