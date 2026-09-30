from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]) & (F.col("method") == "paypal") & (F.col("status") == "REFUNDED"))
    .select("id", "amount", "order_id", F.trim("status").alias("clean_status"))
    .orderBy("order_id", "id")
    .limit(5)
)
