from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.max_amount") >= 2400)
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("at.max_amount"),
        F.col("at.merchant_list"),
    )
    .orderBy(F.col("balance").desc())
)
