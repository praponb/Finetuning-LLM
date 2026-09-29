from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-06-01", "2025-02-01"))
    .select(
        "id",
        "opened_date",
        "balance",
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
    )
)
