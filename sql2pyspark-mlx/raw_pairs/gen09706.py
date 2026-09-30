from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", "txn_type")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)
