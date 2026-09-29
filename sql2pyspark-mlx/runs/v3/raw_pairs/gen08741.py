from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "OPEN"]) & F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]))
)
