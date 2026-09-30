from pyspark.sql import functions as F

remaining_keys = (
    df_transactions
    .select("id", "merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("merchant").isin(["Amazon", "Shell"]))
        .select("id", "merchant")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("amount") >= 4500, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("txn_type") == "CREDIT").alias("credit_events"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
    )
)
