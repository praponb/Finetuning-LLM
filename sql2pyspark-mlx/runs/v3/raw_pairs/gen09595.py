from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & F.col("status").isin(["FAILED", "REFUNDED"]) & (F.col("amount") >= 1210))
    .withColumn("status_upper", F.upper("status"))
    .orderBy(F.col("order_id").desc(), "paid_at")
)
