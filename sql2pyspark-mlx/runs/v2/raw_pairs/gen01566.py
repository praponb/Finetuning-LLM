from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([14, 32]) & (F.col("status") == "CLOSED"))
    .select(
        "customer_id",
        "balance",
        "account_type",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
    )
)
