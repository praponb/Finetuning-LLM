from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 16)
    .select("account_type", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("status") == "FROZEN")
        .select("account_type", "customer_id")
    )
)
