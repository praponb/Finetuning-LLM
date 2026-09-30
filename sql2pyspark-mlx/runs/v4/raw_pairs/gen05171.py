from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-07-08")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("txn_type").alias("txn_type_upper"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("amount") >= 4100, "LARGE").otherwise("SMALL").alias("amount_tier"),
        "txn_type",
    )
    .agg(
        F.avg(F.when(F.col("merchant") == "Starbucks", F.col("amount"))).alias("starbucks_avg_amount"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .orderBy("amount_tier")
)
