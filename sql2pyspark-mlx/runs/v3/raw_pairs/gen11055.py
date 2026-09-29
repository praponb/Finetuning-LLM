from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.year("opened_date") >= 2024) & F.col("account_type").isin(["SAVINGS", "CREDIT"]))
    .withColumn("balance_whole", F.floor("balance"))
    .withColumn("balance_rounded", F.round("balance", 2))
)
