from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNull())
    .select("id", "opened_date", "status")
    .unionByName(
        df_accounts
        .filter(F.col("status") != "FROZEN")
        .select("id", "opened_date", "status")
    )
    .distinct()
)
