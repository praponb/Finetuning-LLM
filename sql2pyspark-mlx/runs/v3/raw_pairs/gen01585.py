from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(65000, 78000))
    .select("account_type", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") < "2025-09-20")
        .select("account_type", "opened_date")
    )
)
