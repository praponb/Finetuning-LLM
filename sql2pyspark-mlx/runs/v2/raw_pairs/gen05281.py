from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", "balance", F.abs("balance").alias("abs_balance"))
    .orderBy(F.col("balance").desc())
)
