from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") != "Amazon") & (F.col("txn_time") < "2025-06-07"))
    .groupBy("txn_type")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0)).alias("grab_count"),
    )
    .orderBy(F.col("stddev_amount").desc())
)
