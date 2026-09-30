from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-07-01", "2024-07-01"))
    .select("account_type", "status", "opened_date")
    .orderBy("account_type")
)
