from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type")
    .orderBy("txn_type")
)
