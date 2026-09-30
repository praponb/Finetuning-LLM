from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter((F.col("txn_time") < "2024-01-05") & F.col("account_id").isin([21, 36]))
    .withColumn("txn_type_upper", F.upper("txn_type"))
)

agg = (
    filtered_transactions
    .groupBy("txn_type")
    .agg(F.count("*").alias("num_transactions"))
)

result = (
    agg
    .filter(F.col("num_transactions") > 7)
    .orderBy(F.col("txn_type").desc())
)
