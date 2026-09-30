from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2023-04-18") & (F.col("account_id") == 33) & (F.col("amount") < 1700))
    .groupBy("merchant", F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
    )
    .filter(F.col("grab_count") > 3)
    .drop("grab_count")
    .orderBy(F.col("txn_quarter").desc(), F.col("avg_amount").desc())
)
