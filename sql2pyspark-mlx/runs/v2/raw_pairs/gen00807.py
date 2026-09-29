from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 47000) & (F.col("status") == "OPEN") & F.col("customer_id").isNull())
    .withColumn("status_account_type", F.concat("status", F.lit(" "), "account_type"))
    .withColumn("balance_whole", F.ceil("balance"))
)
