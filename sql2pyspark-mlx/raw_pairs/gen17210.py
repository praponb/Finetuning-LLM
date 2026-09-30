from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .groupBy("account_type")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 5)
)
