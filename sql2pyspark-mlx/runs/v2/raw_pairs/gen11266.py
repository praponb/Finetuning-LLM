from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

transactions_stats = (
    filtered_transactions
    .groupBy("merchant")
    .agg(F.count("amount").alias("amount_count"))
)

result = transactions_stats.orderBy("merchant")
