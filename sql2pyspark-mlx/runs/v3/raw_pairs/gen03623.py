from pyspark.sql import functions as F

src = df_payments.filter(F.col("order_id") == 29)

status_summary = (
    src
    .groupBy("status", "method")
    .agg(F.max("amount").alias("max_amount"))
)

result = status_summary.filter(F.col("max_amount") < 1480)
