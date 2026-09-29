from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("min_amount") > 3300)
)
