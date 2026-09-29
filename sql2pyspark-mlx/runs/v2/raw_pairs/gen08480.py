from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") >= 9)
    .select("account_id", "amount_count")
    .orderBy("account_id")
)
