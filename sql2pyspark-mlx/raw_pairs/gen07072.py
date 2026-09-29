from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("method") == "paypal") & (F.col("paid_at") < "2023-08-01")) | F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
    .orderBy(F.col("paid_at").desc())
)
