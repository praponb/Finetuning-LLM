from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(24000, 44000) & (F.col("opened_date") <= "2025-09-09"))
    .select("account_type", "customer_id", "balance")
    .orderBy(F.col("balance").desc())
)
