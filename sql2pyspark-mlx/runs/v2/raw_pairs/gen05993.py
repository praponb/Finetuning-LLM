from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "CREDIT")
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") >= 19000)
    .select("account_id", "total_amount")
    .orderBy(F.col("total_amount").desc())
)
