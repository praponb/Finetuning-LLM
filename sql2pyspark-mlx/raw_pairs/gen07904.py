from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-08-13", "2025-02-18"))
    .select("status")
    .unionByName(df_payments.select("status"))
)
