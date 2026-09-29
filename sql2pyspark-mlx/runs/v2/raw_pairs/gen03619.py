from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]) & (F.col("account_type") != "SAVINGS"))
    .select("id", "balance", "status")
    .orderBy(F.col("balance").desc(), "id")
)
