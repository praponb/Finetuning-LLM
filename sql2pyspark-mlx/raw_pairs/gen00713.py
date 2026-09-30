from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("method") == "paypal") & F.col("amount").between(1290, 1330)) | (F.col("method") == "card"))
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("amount_range") >= 71)
)
