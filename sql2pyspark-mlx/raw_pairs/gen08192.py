from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") < 63000) & ((F.col("account_type") == "SAVINGS") | F.col("opened_date").between("2023-07-01", "2024-05-01")))
    .select("id", "customer_id")
    .orderBy("id")
)
