from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .select("id", "account_type", "balance", "customer_id")
)
