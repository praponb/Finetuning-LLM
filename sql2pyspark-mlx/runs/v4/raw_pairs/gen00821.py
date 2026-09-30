from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-03-01")
    .select("account_type", "status")
    .distinct()
)
