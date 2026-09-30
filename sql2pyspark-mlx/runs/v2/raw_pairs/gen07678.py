from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "FROZEN"]))
    .select("account_type", "balance", "customer_id")
    .orderBy(F.col("account_type").desc())
)
