from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(35000, 52000))
    .select("account_type", "status")
    .intersect(
        df_accounts
        .filter(F.col("balance") > 46000)
        .select("account_type", "status")
    )
)
