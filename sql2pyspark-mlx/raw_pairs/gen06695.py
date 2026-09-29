from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.length(F.col("c.name")) > 7)
    .select(
        F.col("t.amount"),
        F.col("t.account_id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("c.country"),
        F.initcap(F.trim(F.col("t.txn_type"))).alias("txn_type_title"),
    )
    .orderBy("opened_date")
)
