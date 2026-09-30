from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.hour("paid_at").between(12, 18) & F.col("status").isin(["SUCCESS", "FAILED"]) & (F.col("paid_at") < "2025-01-01")))
    .select(
        "id",
        "status",
        F.lower("method").alias("method_lower"),
        F.round("amount", 2).alias("amount_rounded"),
    )
)
