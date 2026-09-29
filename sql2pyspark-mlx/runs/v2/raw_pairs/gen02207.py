from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter((F.col("t.merchant") == "Shell") & (F.col("t.account_id") == 16))
    .select(
        F.col("t.account_id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("a.balance"),
        F.initcap(F.trim(F.col("a.account_type"))).alias("account_type_title"),
    )
)
