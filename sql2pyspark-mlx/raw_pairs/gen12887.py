from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([24, 33, 37]) & (F.col("txn_type") == "FEE") & (F.col("merchant") == "Lazada"))
    .withColumn("txn_type_upper", F.upper("txn_type"))
)
