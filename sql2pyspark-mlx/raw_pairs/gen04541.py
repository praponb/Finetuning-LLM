from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 3000) & (F.to_date("txn_time") < "2024-01-29"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
