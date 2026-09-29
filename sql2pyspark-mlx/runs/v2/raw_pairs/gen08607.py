from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.account_type") == "SAVINGS") & (F.col("t.merchant") != "Shell"))
    .select(
        F.col("t.merchant"),
        F.col("a.balance"),
        F.col("c.country"),
        F.col("c.segment"),
    )
    .orderBy("segment")
)
