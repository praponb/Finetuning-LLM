from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 28000)
    .select("id", "status", "customer_id", "account_type")
)
