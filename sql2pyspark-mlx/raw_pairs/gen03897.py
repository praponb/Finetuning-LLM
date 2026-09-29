from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", F.year("txn_time").alias("txn_year"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("total_amount") > 16000)
    .orderBy(F.col("max_amount").desc())
)
