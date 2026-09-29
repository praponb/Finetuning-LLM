from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 35]))
    .select("id", "balance", "account_type", "status")
)
