from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-03-01", "2024-07-01") & (F.col("status") == "OPEN"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.quarter("opened_date").alias("opened_date_quarter"),
    )
)
