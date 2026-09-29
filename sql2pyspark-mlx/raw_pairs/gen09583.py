from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.count("*").alias("num_accounts"))
    .filter(F.col("num_accounts") >= 8)
    .select("opened_quarter", "num_accounts")
)
