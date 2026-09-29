from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("balance") >= 18000) & (F.year("opened_date") >= 2025)) | (F.col("account_type") == "CHECKING"))
    .select("id", "status", "customer_id")
)
