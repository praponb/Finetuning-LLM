from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "CHECKING") & (F.col("opened_date") <= "2024-12-16"))
    .select(
        "status",
        "customer_id",
        "balance",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
    .orderBy("status")
)
