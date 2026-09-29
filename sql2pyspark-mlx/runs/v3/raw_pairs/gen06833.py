from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([21, 32]))
    .select("opened_date")
    .unionByName(
        df_accounts
        .filter(F.col("account_type") == "CHECKING")
        .select("opened_date")
    )
    .orderBy("opened_date")
)
