from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("t.merchant") == "Shell")
    .select(
        F.col("t.amount"),
        F.col("t.merchant"),
        F.col("a.account_type"),
        F.col("c.name"),
    )
    .orderBy(F.col("merchant").desc(), F.col("amount").desc())
)
