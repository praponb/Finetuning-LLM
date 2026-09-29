from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "paypal")
    .select("order_id", "status", F.trim("status").alias("clean_status"))
)
