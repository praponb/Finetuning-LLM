from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type", "id")
    .intersect(
        df_accounts
        .filter(F.col("balance") > 43000)
        .select("account_type", "id")
    )
)
