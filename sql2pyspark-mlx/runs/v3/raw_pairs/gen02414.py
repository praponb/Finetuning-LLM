from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~(~F.col("is_flagged") & (F.col("txn_time") >= "2024-12-01") & (F.to_date("txn_time") >= "2025-01-01")))
    .groupBy("is_flagged")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("total_amount", "is_flagged")
    .limit(10)
)
