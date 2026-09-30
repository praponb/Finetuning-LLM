from pyspark.sql import functions as F

base = df_orders.filter(F.col("discount") >= 0.35)

status_summary = (
    base
    .groupBy("status")
    .agg(F.count("*").alias("row_count"))
)

result = status_summary.orderBy(F.col("row_count").desc())
