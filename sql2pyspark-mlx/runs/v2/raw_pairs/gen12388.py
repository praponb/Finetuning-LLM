from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "REFUNDED") | (F.col("amount") < 910))
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.count("*").alias("cnt"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("paid_at").alias("first_paid_at"),
    )
)
