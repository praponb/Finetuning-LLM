from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0)).alias("lost_count"))
    .filter(F.col("lost_count") < 26)
)
