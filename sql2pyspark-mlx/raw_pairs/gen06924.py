from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("age").between(61, 64) | (F.length("email") >= 12))

segment_summary = (
    filtered_customers
    .groupBy("segment")
    .agg(F.round(F.avg("age"), 2).alias("avg_age"))
)

result = segment_summary.orderBy("segment")
