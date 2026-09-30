from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 3900)
)
