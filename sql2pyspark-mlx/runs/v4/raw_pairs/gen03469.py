from pyspark.sql import functions as F

result = (
    df_accounts
    .select("id")
    .intersectAll(
        df_accounts
        .filter(F.col("balance") >= 75000)
        .select("id")
    )
)
