from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-02-09", "2025-07-01") & ((F.col("customer_id") == 32) | F.col("status").isin(["CLOSED", "FROZEN"])))
    .select(
        "id",
        "opened_date",
        "status",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
