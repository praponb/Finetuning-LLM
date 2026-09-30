from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(~((F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]) & F.col("status").isin(["FAILED", "REFUNDED"])) | (F.col("amount") <= 1120)))
    .withColumn("method_status", F.concat("method", F.lit(" "), "status"))
)

payments_stats = (
    recent_payments
    .groupBy(F.year("paid_at").alias("paid_year"), "method")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("paid_at").alias("first_paid_at"),
    )
)

result = (
    payments_stats
    .filter(F.col("amount_range") > 42)
    .select("paid_year", "method", "amount_range", "first_paid_at")
)
