from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED"]) | F.col("amount").isNull())
    .select("id", "status")
)
