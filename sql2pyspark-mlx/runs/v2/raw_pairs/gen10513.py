from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "txn_time",
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
    )
    .orderBy(F.col("txn_time").desc())
)
