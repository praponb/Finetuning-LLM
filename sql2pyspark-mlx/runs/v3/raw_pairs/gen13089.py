from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]) & (F.col("opened_date") > "2024-10-01") & (F.year("opened_date") == 2024))
    .select(
        "balance",
        "customer_id",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)
