from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]) & F.col("account_type").isin(["SAVINGS", "LOAN"]))
    .select("id", "balance", F.length("status").alias("status_length"))
)
