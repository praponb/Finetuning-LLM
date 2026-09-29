from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy(F.col("account_type").desc())
    .limit(3)
)
