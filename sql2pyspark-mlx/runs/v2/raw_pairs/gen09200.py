from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "REFUNDED") & (F.col("method") == "promptpay"))
    .select("order_id", "status", F.col("amount").cast("int").alias("amount_int"))
    .orderBy("status", F.col("amount_int").desc())
)
