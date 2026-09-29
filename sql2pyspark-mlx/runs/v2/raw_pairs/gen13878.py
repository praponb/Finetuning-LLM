from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 9) | F.col("account_id").isin([11, 13, 32]))
    .select("amount", F.round((F.col("amount") * 0.85), 2).alias("amount_scaled"))
    .orderBy(F.col("amount_scaled").desc(), "amount")
)
