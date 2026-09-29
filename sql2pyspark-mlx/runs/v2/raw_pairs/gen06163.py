from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") < 7)
    .select("end_month", "row_count")
)
