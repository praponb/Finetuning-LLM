from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.status").isin(["FROZEN", "OPEN"]))
    .groupBy(F.col("c.country"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "CREDIT", F.col("t.amount")).otherwise(0)).alias("credit_amount"),
    )
    .filter(F.col("credit_amount") > 25)
    .orderBy("country")
)
