from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("balance") < 47000) & F.col("balance").between(56000, 73000)))
    .select("account_type", "status", "customer_id")
)
