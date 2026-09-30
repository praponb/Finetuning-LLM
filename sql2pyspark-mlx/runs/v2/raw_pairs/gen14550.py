from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("balance").between(19000, 24000))
    .select("account_type")
)
