from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(F.countDistinct("balance").alias("unique_account_types"))
    .filter(F.col("unique_account_types") >= 9)
    .orderBy(F.col("unique_account_types").desc())
)
