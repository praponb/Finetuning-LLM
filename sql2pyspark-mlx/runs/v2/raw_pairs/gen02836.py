from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 24)
    .select("opened_date", "account_type")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id") == 30)
        .select("opened_date", "account_type")
    )
    .distinct()
    .orderBy("opened_date")
)
