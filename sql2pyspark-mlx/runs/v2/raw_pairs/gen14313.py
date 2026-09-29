from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-05-15", "2025-05-01"))
    .select("status", "account_type")
    .distinct()
)
