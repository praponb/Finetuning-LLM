from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("balance") >= 40000) & (F.col("opened_date") >= "2024-03-01") & F.col("customer_id").isNull())
    .withColumn("account_type_status_key", F.concat_ws("-", "account_type", "status"))
)

account_type_summary = (
    filtered_accounts
    .groupBy("account_type")
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = account_type_summary.select("account_type", "first_opened_date")
