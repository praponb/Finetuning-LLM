from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "CREDIT"]))
    .select("amount", F.round((F.col("amount") * 1.1), 2).alias("amount_adjusted"))
)
