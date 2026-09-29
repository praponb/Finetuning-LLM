from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "account_type",
        "status",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
    )
)
