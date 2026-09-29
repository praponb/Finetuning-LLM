from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .filter(F.col("A.shell_count") > 77)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("A.merchant_values"),
        F.col("A.shell_count"),
    )
    .orderBy(F.col("balance").desc())
)
