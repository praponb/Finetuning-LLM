from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "SAVINGS") & F.col("balance").isNotNull())
    .select("id", "balance")
)
