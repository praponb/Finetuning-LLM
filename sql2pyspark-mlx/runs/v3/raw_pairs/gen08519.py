from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.countDistinct("balance").alias("unique_account_types"))
    .orderBy("status")
)
