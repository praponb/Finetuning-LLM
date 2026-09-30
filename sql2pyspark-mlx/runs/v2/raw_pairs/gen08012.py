from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2023-09-30")
    .select(
        "id",
        "balance",
        "opened_date",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled_2"),
    )
)
