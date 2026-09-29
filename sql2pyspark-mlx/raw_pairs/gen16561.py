from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") == 4))
    .select(
        "id",
        "balance",
        "status",
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
    .orderBy("status_filled")
)
