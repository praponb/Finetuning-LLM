from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]) & (F.col("amount") >= 1150))
    .select(
        "status",
        F.coalesce("method", F.lit("Unknown")).alias("method_filled"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
    .orderBy("status")
)
