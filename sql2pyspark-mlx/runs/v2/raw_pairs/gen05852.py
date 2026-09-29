from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CREDIT"]))
    .select("id", "status", "account_type")
    .orderBy("id")
)
