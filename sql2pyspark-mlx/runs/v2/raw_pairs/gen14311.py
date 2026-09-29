from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.status").isin(["CLOSED", "OPEN"]))
    .select(
        F.col("t.amount"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("a.customer_id"),
    )
    .orderBy(F.col("amount").desc(), "opened_date")
)
