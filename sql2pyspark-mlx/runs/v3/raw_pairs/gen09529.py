from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("account_type") == "CREDIT") & (F.col("balance") <= 22000)) | F.col("customer_id").isin([11, 14, 39]))
)
