from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lazada"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.pct_lazada") >= 29)
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("t.pct_lazada"))
    .orderBy(F.col("customer_id").desc())
)
