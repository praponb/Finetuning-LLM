from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS", "LOAN"]) | F.month("opened_date").isin([1, 2, 7]))
    .select(
        "id",
        "account_type",
        "status",
        "balance",
        F.coalesce("status", F.lit("Unknown")).alias("status_filled"),
    )
    .orderBy("status_filled")
)
