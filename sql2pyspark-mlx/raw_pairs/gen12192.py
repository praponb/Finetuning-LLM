from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter((F.col("carrier") != "Kerry") & F.col("shipped_date").between("2024-05-01", "2024-09-01"))
    .withColumn("cost_rounded", F.round("cost", 0))
)

delivered_month_summary = (
    filtered_shipments
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0)).alias("lost_count"))
)

result = (
    delivered_month_summary
    .filter(F.col("lost_count") > 76)
    .select("delivered_month", "lost_count")
    .orderBy("delivered_month")
    .limit(50)
)
