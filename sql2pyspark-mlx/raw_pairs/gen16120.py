from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= "2025-10-01")
    .select(
        "*",
        F.upper("account_type").alias("account_type_upper"),
        F.concat("status", F.lit(", "), "account_type").alias("status_account_type"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

result = (
    cleaned
    .select("status", "id")
    .intersect(
        df_accounts
        .filter(F.col("balance").between(52000, 56000))
        .select("status", "id")
    )
)
