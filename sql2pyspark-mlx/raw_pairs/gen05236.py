from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 4)
)
