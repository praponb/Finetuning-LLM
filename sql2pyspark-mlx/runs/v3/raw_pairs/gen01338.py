from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type").isin(["CHECKING", "LOAN"]))
    .select(
        F.col("t.txn_time"),
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
)
