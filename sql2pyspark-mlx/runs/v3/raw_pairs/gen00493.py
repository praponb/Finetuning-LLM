from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "CLOSED"]))
    .select(
        "id",
        "status",
        "account_type",
        "opened_date",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)
