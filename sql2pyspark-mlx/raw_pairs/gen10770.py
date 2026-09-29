from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CHECKING") & F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select(
        "id",
        "account_type",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
    .orderBy(F.col("days_since_opened_date").desc())
)
