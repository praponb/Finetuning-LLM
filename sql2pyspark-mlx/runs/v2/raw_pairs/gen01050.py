from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select("id", "opened_date", "status")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") <= "2023-11-01")
        .select("id", "opened_date", "status")
    )
    .orderBy("opened_date")
)
