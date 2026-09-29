from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(~(F.col("a.balance").between(64000, 72000) & (F.col("a.balance") <= 56000)))
    .select(
        F.col("c.segment"),
        F.col("a.customer_id"),
        F.concat_ws("-", F.col("a.status"), F.col("a.account_type")).alias("status_account_type_key"),
    )
    .orderBy(F.col("customer_id").desc())
)
