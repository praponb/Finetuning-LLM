from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant", "txn_type")
    .distinct()
    .orderBy("txn_type", "merchant")
    .limit(50)
)
