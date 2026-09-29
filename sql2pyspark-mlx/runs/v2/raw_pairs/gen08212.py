from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2024-06-01")
    .groupBy("is_flagged", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.countDistinct("amount").alias("unique_accounts"))
    .orderBy("unique_accounts", "txn_month")
)
