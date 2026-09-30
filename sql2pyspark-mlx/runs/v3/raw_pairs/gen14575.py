from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "LOAN", "CREDIT"]))
    .withColumn("clean_status", F.trim("status"))
)

account_type_summary = (
    base
    .groupBy("account_type")
    .agg(F.max("opened_date").alias("last_opened_date"))
)

result = (
    account_type_summary
    .select("account_type", "last_opened_date")
    .orderBy("last_opened_date")
)
