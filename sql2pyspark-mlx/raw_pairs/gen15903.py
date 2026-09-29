from pyspark.sql import functions as F

result = (
    df_accounts
    .select("id")
    .intersect(
        df_accounts
        .filter(F.col("status").isin(["OPEN", "CLOSED"]))
        .select("id")
    )
)
