from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "OPEN") | ~F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .groupBy("account_type")
    .agg(F.count("*").alias("account_count"))
    .filter(F.col("account_count") > 3)
    .orderBy(F.col("account_count").desc(), "account_type")
)
