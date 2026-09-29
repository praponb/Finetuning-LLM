from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 6) & F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .select("id", "status", F.col("amount").cast("int").alias("amount_int"))
    .orderBy(F.col("amount_int").desc(), "id")
    .limit(1)
)
