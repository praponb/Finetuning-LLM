from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "CHECKING")
    .select("opened_date", "account_type")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isin([2, 8, 17]))
        .select("opened_date", "account_type")
    )
)
