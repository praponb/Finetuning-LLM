from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("status").isin(["FAILED", "SUCCESS"]) & F.col("method").isNotNull()))
    .select("id", "paid_at", "status")
)
