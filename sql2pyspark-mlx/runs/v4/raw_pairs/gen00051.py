from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("txn_type", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("merchant") == "7-Eleven")
        .select("txn_type", "account_id")
    )
)

verified = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("amount") >= 3500, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.count_if(F.col("merchant") == "Grab").alias("grab_events"),
    )
    .orderBy(F.col("amazon_amount").desc_nulls_first())
)
