from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .select(
        "id",
        "paid_at",
        "amount",
        F.to_date("paid_at").alias("paid_at_date"),
        F.upper("method").alias("method_upper"),
    )
)
