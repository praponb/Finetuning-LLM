from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]) & (F.col("status") == "SUCCESS") & (F.col("status") == "REFUNDED"))
    .select("id", "method", F.length("status").alias("status_length"))
)
