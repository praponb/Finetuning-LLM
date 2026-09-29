from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CREDIT"]) & (F.col("balance").between(57000, 61000) | F.col("customer_id").isNotNull()))
)
