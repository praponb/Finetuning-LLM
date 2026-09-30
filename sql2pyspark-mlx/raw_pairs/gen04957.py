from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .groupBy("status", F.year("opened_date").alias("opened_year"))
    .agg(F.count("balance").alias("balance_count"))
)
