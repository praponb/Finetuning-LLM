from pyspark.sql import functions as F

result = (
    df_payments
    .cube(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.when(F.col("amount") >= 1340, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("method") == "card", F.col("amount"))).alias("card_total_amount"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("amount_per_failed"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
    )
    .filter(F.col("amount_p95") > 0)
    .orderBy(F.col("amount_tier").desc())
)
