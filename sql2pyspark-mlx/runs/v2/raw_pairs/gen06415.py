from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-11-01", "2024-11-01") & F.col("customer_id").isNull())
    .select("id", "account_type", "customer_id", "balance")
)
