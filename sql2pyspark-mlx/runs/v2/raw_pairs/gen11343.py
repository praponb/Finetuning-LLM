from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .select("id", "balance", "account_type")
    .orderBy(F.col("id").desc())
    .limit(5)
)
