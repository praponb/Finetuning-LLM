from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 17) & (F.col("is_flagged") | F.col("amount").between(1100, 2100)))
    .orderBy("id")
)
