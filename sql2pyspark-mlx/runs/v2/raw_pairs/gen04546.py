from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 2500) & ((F.col("amount") > 1200) | F.col("txn_type").isin(["FEE", "CREDIT"])))
)
