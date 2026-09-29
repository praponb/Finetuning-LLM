from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-03-07", "2025-07-01"))
    .select("account_type", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("balance") < 78000)
        .select("account_type", "opened_date")
    )
    .orderBy("opened_date")
)
