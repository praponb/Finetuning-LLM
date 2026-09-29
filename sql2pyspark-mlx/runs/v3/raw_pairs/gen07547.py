from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & (F.col("balance") <= 64000))
    .select("id", "opened_date", "status", "customer_id")
)
