from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "status")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 5300)
)
