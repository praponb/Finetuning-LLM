from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 5) & (F.col("customer_id").isNotNull() | (F.col("account_type") != "LOAN")))
    .select(
        "id",
        "account_type",
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
    )
    .orderBy("account_type")
)
