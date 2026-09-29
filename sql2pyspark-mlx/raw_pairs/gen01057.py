from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"), "brand")
    .agg(F.max("launch_date").alias("last_launch_date"))
)
