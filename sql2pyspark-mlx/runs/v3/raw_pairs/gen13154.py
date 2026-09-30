from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("amount") < 3500) & F.col("account_id").isin([1, 3, 7, 21]) & F.hour("txn_time").between(6, 14)))
    .select("id", "account_id")
)
