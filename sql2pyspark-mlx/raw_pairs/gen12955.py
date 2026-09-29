from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter((F.col("t.amount") <= 2700) | ~F.col("a.account_type").isin(["SAVINGS", "LOAN"]))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.account_id"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
    .orderBy(F.col("account_id").desc(), "id")
    .limit(50)
)
