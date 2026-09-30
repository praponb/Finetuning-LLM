from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CREDIT", "SAVINGS"]) | F.col("account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]))
    .select(
        "customer_id",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
)
