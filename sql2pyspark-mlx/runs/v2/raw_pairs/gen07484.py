from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & (F.col("account_type") == "LOAN"))
    .withColumn("status_lower", F.lower("status"))
)
