from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.lazada_amount") > 69)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("t.credit_amount"),
        F.col("t.lazada_amount"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
