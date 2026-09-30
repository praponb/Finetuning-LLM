from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"))
    .filter(F.col("closed_count") > 1)
    .orderBy(F.col("status").desc())
)
