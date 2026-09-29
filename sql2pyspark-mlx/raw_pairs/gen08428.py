from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 33)
    .select(
        "id",
        "account_type",
        "customer_id",
        (F.datediff(F.current_date(), "opened_date") / 365).alias("years_since_opened_date"),
    )
)
