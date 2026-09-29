from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(~F.col("is_flagged")).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.customer_id").isin([36, 38]))
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("a.opened_date"),
    )
    .orderBy("customer_id", F.col("balance").desc(), "id")
    .limit(1)
)
