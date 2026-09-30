from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 56000)
    .select("opened_date", "status")
    .unionByName(
        df_accounts
        .filter(F.col("balance") < 45000)
        .select("opened_date", "status")
    )
)
