from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance").between(33000, 58000) & (F.col("account_type") == "CHECKING")) | (F.col("opened_date") <= "2025-09-01"))
    .groupBy(
        "status",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "account_type",
    )
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
)
