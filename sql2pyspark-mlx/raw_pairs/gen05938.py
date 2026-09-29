from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1200, 3300))
    .groupBy("is_flagged")
    .agg(F.count("amount").alias("amount_count"))
)
