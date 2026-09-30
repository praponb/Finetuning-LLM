from pyspark.sql import functions as F

kept_keys = (
    df_accounts
    .select("status", "id")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["CLOSED", "FROZEN"]))
        .select("status", "id")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube("status")
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_rows"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles_2"),
        F.grouping("status").alias("grouping_id"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
