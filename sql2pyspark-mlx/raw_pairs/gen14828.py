from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0)).alias("grab_count"),
    )
    .filter(F.col("grab_count") > 67)
    .drop("grab_count")
    .orderBy("txn_type", F.col("cnt").desc())
)
