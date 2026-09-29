from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 22000)
    .select("opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") >= "2023-07-01")
        .select("opened_date")
    )
    .orderBy("opened_date")
)
