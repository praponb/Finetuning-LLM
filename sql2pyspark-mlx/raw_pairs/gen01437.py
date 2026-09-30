from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter((F.col("a.balance") >= 32000) & F.col("c.city").isin(["New York", "Chiang Mai"]))
    .select(
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("c.name"),
        F.col("c.country"),
    )
    .orderBy(F.col("name").desc(), "status")
)
