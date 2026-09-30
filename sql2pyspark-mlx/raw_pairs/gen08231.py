from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNull())
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.month("start_date").isin([7, 8, 9]))
        .select("status")
    )
    .orderBy("status")
)
