from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"))
    .filter(F.col("completed_count") > 4)
    .select("status", "order_month", "completed_count")
    .orderBy(F.col("status").desc(), "order_month")
    .limit(5)
)
