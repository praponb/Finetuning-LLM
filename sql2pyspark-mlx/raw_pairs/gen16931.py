from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([13, 34, 35]) & F.col("merchant").isNull())
    .agg(F.count("amount").alias("amount_count"))
    .orderBy(F.col("amount_count").desc())
)
