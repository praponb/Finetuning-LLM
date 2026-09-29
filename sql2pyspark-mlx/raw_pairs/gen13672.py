from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("amount") >= 3100) & ((F.col("txn_type") != "FEE") | F.col("merchant").isNotNull())))
)
