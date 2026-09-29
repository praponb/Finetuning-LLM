from pyspark.sql import functions as F

result = (
    df_accounts
    .select("balance", "customer_id", F.ceil("balance").alias("balance_whole"))
    .orderBy(F.col("balance").desc(), F.col("balance_whole").desc())
)
