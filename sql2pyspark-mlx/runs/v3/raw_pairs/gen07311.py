from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") != "REFUNDED")
    .select("id", "method", F.month("paid_at").alias("paid_at_month"))
)
