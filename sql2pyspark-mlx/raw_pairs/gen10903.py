from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 52000)
    .select("balance", "customer_id", F.floor("balance").alias("balance_whole"))
    .orderBy(F.col("customer_id").desc())
)
