from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
    .filter(F.col("stddev_amount") < 44)
)
