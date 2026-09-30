from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(~F.col("is_flagged")).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.opened_date"),
    )
)
