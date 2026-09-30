from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2023-09-30")
    .select("opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("account_type") != "SAVINGS")
        .select("opened_date")
    )
    .distinct()
    .orderBy("opened_date")
)
