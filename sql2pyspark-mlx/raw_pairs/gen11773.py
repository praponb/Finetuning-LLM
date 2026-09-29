from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select("is_flagged", "txn_type", "amount")
    .orderBy("amount")
)
