from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(47000, 58000) & (F.col("opened_date").between("2023-05-01", "2024-02-01") | (F.col("customer_id") == 25)))
    .select("balance", "status")
    .orderBy(F.col("balance").desc())
)
