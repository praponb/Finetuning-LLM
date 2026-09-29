from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.year("txn_time").alias("txn_year"),
        F.month("txn_time").alias("txn_month"),
    )
    .agg(F.min("amount").alias("min_amount"))
    .orderBy(F.col("min_amount").desc())
)
