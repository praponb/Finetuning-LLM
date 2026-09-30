from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") == "CREDIT") & (F.col("txn_time") >= "2025-04-01"))
    .groupBy("merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("*").alias("cnt"),
    )
    .orderBy("merchant")
)
