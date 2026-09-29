from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.pct_shell") > 76)
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("t.pct_shell"))
    .orderBy("id", F.col("customer_id").desc())
)
