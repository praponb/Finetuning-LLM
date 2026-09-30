from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("amount").isNotNull()).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("a.customer_id"))
    .orderBy(F.col("customer_id").desc())
)
