from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "is_flagged", "txn_time")
    .orderBy("txn_time", "id")
    .limit(1)
)
