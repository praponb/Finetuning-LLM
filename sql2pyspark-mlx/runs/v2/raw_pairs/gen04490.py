from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-02-01")
    .select("customer_id", "balance", "status")
)
