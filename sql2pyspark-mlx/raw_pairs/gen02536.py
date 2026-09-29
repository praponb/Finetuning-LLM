from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 2)
    .select("amount")
    .intersect(
        df_subscriptions
        .filter(F.col("monthly_fee").between(110, 160))
        .select("monthly_fee")
    )
)
