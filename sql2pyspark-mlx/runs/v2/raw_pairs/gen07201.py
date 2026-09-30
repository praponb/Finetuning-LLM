from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2023-09-09")
    .select("merchant")
    .distinct()
)
