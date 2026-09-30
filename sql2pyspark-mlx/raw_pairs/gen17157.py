from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "paypal"]) & ((F.col("status") == "FAILED") | F.col("status").isin(["REFUNDED", "FAILED"])))
    .select(
        "paid_at",
        "method",
        F.to_date("paid_at").alias("paid_at_date"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy("paid_at_date", "paid_at")
)
