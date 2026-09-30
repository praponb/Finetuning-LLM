from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("status")
    )
    .orderBy("status")
)
