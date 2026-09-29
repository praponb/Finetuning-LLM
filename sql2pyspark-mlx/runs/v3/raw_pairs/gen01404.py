from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)

paid_month_summary = (
    base
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        (F.max("amount_filled") - F.min("amount_filled")).alias("amount_filled_range"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    paid_month_summary
    .filter(F.col("min_amount") >= 510)
    .orderBy(F.col("amount_filled_range").desc())
)
