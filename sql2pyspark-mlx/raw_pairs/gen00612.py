from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.status") == "FROZEN") & F.col("a.account_type").isin(["LOAN", "CREDIT", "CHECKING"]))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("a.opened_date"),
        F.col("c.segment"),
        F.col("c.country"),
    )
    .orderBy("segment")
)
