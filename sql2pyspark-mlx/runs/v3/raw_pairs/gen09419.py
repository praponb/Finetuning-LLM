from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.hour("txn_time").between(12, 17) | (F.col("amount") <= 2000)))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
)
