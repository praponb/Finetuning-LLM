from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.customer_id").isNotNull())
    .select(
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("t.is_flagged"),
        F.coalesce(F.col("t.amount"), F.lit(0)).alias("amount_filled"),
    )
)
