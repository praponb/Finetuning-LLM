from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant") != "Grab")
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .filter(F.col("tba.max_amount") >= 3100)
    .select(
        F.col("a.balance"),
        F.col("a.status"),
        F.col("tba.merchant_values"),
        F.col("tba.max_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
