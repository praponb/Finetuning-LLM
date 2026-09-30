from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("account_type", "status")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
        .select("account_type", "status")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("account_type")
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_total_balance"),
    )
)
