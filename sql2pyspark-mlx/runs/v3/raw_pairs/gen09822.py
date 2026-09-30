from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.countDistinct("balance").alias("unique_account_types"))
    .filter(F.col("unique_account_types") >= 1)
)
