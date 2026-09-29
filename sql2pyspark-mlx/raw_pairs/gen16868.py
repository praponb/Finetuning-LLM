from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("paid_at_weekofyear", F.weekofyear("paid_at"))
    .withColumn("method_clean", F.coalesce("method", F.lit("DIRECT")))
)

result = (
    valid
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1710, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .groupingSets(
        [["year_start", "amount_tier", "quarter_start"], ["year_start", "amount_tier"], ["year_start"]],
        "year_start",
        "amount_tier",
        "quarter_start",
    )
    .agg(
        F.max(F.when(F.col("status") == "REFUNDED", F.col("amount"))).alias("refunded_max_amount"),
        F.max("paid_at").alias("ended_at"),
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("paid_at_weekofyear")).otherwise(0)).alias("bank_transfer_paid_at_weekofyear"),
        F.grouping("year_start").alias("gid"),
    )
)
