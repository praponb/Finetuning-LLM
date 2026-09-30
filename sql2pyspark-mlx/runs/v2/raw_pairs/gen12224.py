from pyspark.sql import functions as F

result = (
    df_payments
    .filter((~F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & F.col("amount").between(720, 1500)) | (F.col("paid_at") >= "2024-12-27"))
    .select(
        "paid_at",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.ceil("amount").alias("amount_whole"),
    )
    .orderBy("paid_at", F.col("is_refunded").desc())
)
