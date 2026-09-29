from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(F.count("amount").alias("amount_count"))
    .orderBy("amount_count")
)
