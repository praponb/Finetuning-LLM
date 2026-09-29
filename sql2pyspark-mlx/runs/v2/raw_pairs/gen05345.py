from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "CREDIT")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"))
    .filter(F.col("credit_count") > 5)
    .orderBy("opened_quarter")
    .limit(50)
)
