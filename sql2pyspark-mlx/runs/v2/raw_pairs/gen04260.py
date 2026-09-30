from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2025-04-03") & (F.col("opened_date") < "2025-03-01"))
    .select(
        "account_type",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
)
