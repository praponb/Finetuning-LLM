from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
    )
    .filter(F.col("grab_amount") > 32)
    .orderBy(F.col("account_id").desc())
)
