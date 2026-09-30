from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(F.countDistinct("balance").alias("unique_statuss"))
    .orderBy("unique_statuss")
)
