from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNull())
    .select(
        "status",
        "balance",
        F.lower("status").alias("status_lower"),
        F.coalesce("status", F.lit("Unknown")).alias("status_filled"),
    )
)
