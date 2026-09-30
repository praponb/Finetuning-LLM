from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("transaction_count"))
)
