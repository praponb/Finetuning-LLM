from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
    .filter(F.col("unique_txn_types") > 8)
    .orderBy(F.col("txn_month").desc())
)
