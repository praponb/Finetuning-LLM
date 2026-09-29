from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "FEE")
    .groupBy("account_id")
    .agg(
        F.count("*").alias("row_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 2500)
    .orderBy("min_amount")
)
