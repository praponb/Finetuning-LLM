from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select("account_type")
    .orderBy("account_type")
)
