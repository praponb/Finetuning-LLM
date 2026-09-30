from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-06-01", "2024-03-01") & (F.col("status") == "OPEN"))
    .select("balance", "account_type", "status")
    .orderBy("status", "account_type")
)
