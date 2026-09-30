from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2023-09-01") | ~F.col("status").isin(["FROZEN", "CLOSED"]))
    .groupBy("account_type", "status")
    .agg(F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"))
    .orderBy("account_type", "status")
)
