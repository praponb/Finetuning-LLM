from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .intersectAll(
        df_accounts
        .filter(F.col("status") == "FROZEN")
        .select("status")
    )
)
