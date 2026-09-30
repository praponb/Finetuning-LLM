from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & F.col("is_flagged"))
    .groupBy(F.to_date("txn_time").alias("txn_day"), "txn_type")
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("txn_type").desc(), F.col("cnt").desc())
)
