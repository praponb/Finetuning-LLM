from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-09-01", "2025-01-14"))
    .select("status")
    .unionByName(df_subscriptions.select("status"))
)
