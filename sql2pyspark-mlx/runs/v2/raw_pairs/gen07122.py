from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("opened_date") >= "2024-03-12") & F.col("balance").between(51000, 79000)))
    .select("id", "account_type", F.col("balance").cast("int").alias("balance_int"))
)
