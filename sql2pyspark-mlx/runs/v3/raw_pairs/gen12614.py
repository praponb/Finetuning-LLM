from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS", "CREDIT"]) & (F.col("account_type") == "CHECKING"))
    .select(
        "id",
        "account_type",
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
    )
    .orderBy("account_type")
)
