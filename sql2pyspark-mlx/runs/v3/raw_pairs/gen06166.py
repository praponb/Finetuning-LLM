from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-11-20")
    .select("id", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") < "2023-07-16")
        .select("id", "opened_date")
    )
    .distinct()
)
