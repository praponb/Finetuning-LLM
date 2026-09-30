from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & ((F.col("order_id") == 15) | (F.col("amount") >= 1390)))
    .groupBy("order_id", "status")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") >= 3)
    .orderBy(F.col("order_id").desc(), "status")
    .limit(50)
)
