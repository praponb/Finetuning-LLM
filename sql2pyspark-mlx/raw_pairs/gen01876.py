from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "LOAN")
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isin([13, 23]))
        .select("status")
    )
)
