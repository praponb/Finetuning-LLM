from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2023-04-01") & F.col("account_id").isin([23, 28]) & (F.col("txn_type") == "DEBIT"))
    .select("account_id")
)
