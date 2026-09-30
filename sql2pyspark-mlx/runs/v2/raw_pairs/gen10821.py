from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 42000)
    .groupBy("account_type", "status")
    .agg(F.min("opened_date").alias("first_opened_date"))
)
