from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(~F.col("c.segment").isin(["Consumer", "SMB"]))
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.amount"),
        F.col("a.opened_date"),
        F.col("c.credit_limit"),
        F.greatest(F.col("c.credit_limit"), F.col("c.age")).alias("greatest_credit_limit_age"),
    )
    .orderBy(F.col("amount").desc())
)
