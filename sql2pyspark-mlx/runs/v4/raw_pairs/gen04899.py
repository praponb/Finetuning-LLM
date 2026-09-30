from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(((F.col("txn_type") != "DEBIT") & (F.col("txn_time") < "2025-10-06")) | F.col("account_id").isin([9, 23, 24, 29]))
)

agg = (
    valid_transactions
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
    )
)

result = (
    agg
    .select("txn_month", "grab_amount")
    .orderBy("txn_month", F.col("grab_amount").desc())
)
