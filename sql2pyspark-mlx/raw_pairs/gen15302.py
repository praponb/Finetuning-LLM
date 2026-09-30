from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Lazada"]))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.substring("merchant", 1, 3).alias("merchant_prefix"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("amount") >= 4000, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
