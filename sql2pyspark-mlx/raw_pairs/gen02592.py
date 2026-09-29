from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS"]))
    .select(
        "id",
        "customer_id",
        "balance",
        (F.datediff(F.current_date(), "opened_date") / 7).alias("weeks_since_opened_date"),
    )
)
