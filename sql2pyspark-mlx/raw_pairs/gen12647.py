from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.count("*").alias("transaction_count"))
    .orderBy("transaction_count")
)
