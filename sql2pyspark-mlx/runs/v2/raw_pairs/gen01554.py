from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 23, 30]))
    .select("id", "balance", F.abs("balance").alias("abs_balance"))
    .orderBy(F.col("balance").desc())
)
