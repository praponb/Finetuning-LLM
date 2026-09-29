from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.countDistinct("balance").alias("unique_account_types"))
    .orderBy("unique_account_types", F.col("account_type").desc())
)
