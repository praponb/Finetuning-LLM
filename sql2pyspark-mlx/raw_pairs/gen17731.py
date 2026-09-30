from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("cost") < 68)

summary = (
    recent_shipments
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"))
)

result = summary.filter(F.col("kerry_count") > 8)
