from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "merchant",
        "account_id",
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        F.min("txn_time").alias("first_seen"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .filter(F.col("amount_stddev") >= 0)
    .orderBy(F.col("amount_stddev").asc_nulls_last())
)
