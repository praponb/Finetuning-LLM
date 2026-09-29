from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~(~F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]) & (F.col("account_type") == "CHECKING")))
    .groupBy("status")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 5)
)
