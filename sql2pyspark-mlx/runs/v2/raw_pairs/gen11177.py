from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter((F.col("a.account_type") == "CREDIT") & F.col("a.customer_id").isNotNull())
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
    )
    .orderBy(F.col("customer_id").desc())
)
