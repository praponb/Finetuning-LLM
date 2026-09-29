from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["FAILED", "REFUNDED"]))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .orderBy("paid_at", F.col("id").desc())
)
