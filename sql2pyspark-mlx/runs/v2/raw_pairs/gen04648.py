from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1540)
    .groupBy("status", "channel")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("amount")).otherwise(0)).alias("cancelled_amount"),
    )
    .filter(F.col("cancelled_amount") < 23)
)
