from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("t.merchant").isin(["Amazon", "Grab", "Starbucks"]) | F.col("a.account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]))
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("a.customer_id"),
        F.col("c.segment"),
    )
    .orderBy("customer_id")
)
