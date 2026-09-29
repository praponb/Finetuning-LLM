from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "mobile")
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"))
    .filter(F.col("completed_count") >= 2)
)
