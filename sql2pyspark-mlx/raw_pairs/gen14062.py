from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 60000) & (F.col("status") != "OPEN") & F.col("status").isin(["FROZEN", "OPEN"]))
    .select("account_type", "opened_date")
)
