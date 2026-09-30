from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Shell") & F.hour("txn_time").between(7, 11) & F.hour("txn_time").between(10, 13))
)
