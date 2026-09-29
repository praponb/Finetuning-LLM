from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2023-06-04") & (F.col("merchant") == "Grab"))
    .select("account_id", "is_flagged", "amount")
    .orderBy("account_id")
)
