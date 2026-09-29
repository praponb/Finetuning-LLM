from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]))
    .select("opened_date", "account_type")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id") == 33)
        .select("opened_date", "account_type")
    )
)
