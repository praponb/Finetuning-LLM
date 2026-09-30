from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-08-01", "2025-01-01"))
    .select("opened_date", "balance")
)
