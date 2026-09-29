from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.when(F.col("amount") >= 1670, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.max("amount").alias("max_amount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("amount_per_refunded"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .filter(F.col("amount_per_refunded") < 1000000)
    .orderBy(F.col("max_amount").desc())
)
