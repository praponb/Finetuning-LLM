from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"), "brand")
    .agg(F.max("cost").alias("max_cost"))
)
