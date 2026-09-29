from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([7, 15, 37]) & F.col("account_id").isin([17, 34, 37]))
    .withColumn("txn_type_upper", F.upper("txn_type"))
    .withColumn("txn_time_hour", F.hour("txn_time"))
)
