from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]) & ((F.to_date("paid_at") < "2023-11-06") | (F.col("status") == "SUCCESS")))
    .groupBy("method")
    .agg(F.max("paid_at").alias("last_paid_at"))
)
