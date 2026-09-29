from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 34)
    .select("id", "end_date")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("id", "end_date")
    )
)
