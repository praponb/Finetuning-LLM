from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .withColumn("status_account_type_key", F.concat_ws(",", "status", "account_type"))
)
