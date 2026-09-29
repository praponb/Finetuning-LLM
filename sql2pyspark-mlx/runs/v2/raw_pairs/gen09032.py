from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("txn_type").desc())
    .limit(10)
)
