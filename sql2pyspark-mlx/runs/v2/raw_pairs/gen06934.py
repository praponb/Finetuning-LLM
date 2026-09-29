from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") >= 2025)
    .select("opened_date", "customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") <= "2024-03-01")
        .select("opened_date", "customer_id")
    )
)
