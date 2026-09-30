from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
    )
    .filter(F.col("7_eleven_count") > 2)
)
