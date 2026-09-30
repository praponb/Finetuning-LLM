from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-01-01")
    .select(
        "id",
        "balance",
        "customer_id",
        "status",
        F.round("balance", 2).alias("balance_rounded"),
    )
)
