from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") >= 2600)
    .groupBy(
        "txn_type",
        F.date_format("txn_time", "yyyy-MM").alias("txn_month"),
        F.to_date("txn_time").alias("txn_day"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("amount_range").desc(), F.col("txn_day").desc())
)
