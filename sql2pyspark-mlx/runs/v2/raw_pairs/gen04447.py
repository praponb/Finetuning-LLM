from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]) & (F.col("status") != "FAILED") & F.col("status").isin(["FAILED", "SUCCESS"]))
    .select(
        "id",
        "paid_at",
        F.trim("method").alias("clean_method"),
        F.ceil("amount").alias("amount_whole"),
    )
)
