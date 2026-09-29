from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .select("method", "status")
    .unionByName(
        df_payments
        .filter(F.to_date("paid_at") >= "2025-06-01")
        .select("method", "status")
    )
)
