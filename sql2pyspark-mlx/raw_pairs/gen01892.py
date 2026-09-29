from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)
