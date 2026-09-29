from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-04-24").cast("date"))
    .select(
        "id",
        "account_type",
        (F.datediff(F.current_date(), "opened_date") / 7).alias("weeks_since_opened_date"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.account_id").isin([13, 19, 34])), "left_semi")
)
