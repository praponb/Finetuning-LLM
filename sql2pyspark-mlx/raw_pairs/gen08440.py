from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2023-10-01")
    .select(F.lit("transaction").alias("source"), "txn_type")
    .unionByName(
        df_sessions
        .select(F.lit("session").alias("source"), F.col("country").alias("txn_type"))
    )
)
