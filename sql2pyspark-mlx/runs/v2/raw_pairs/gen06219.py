from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", F.year("txn_time").alias("txn_year"))
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum("amount").alias("total_amount"),
    )
)
