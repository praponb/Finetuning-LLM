from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .select("id", "customer_id", "status")
    .intersect(
        df_accounts
        .filter(F.col("opened_date") <= "2024-08-01")
        .select("id", "customer_id", "status")
    )
)
