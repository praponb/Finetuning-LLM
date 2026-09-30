from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0)).alias("grab_count"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .filter(F.col("grab_count") >= 23)
    .select("txn_type", "grab_count", "credit_count")
    .orderBy(F.col("credit_count").desc())
)
