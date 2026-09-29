from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([6, 8, 11]))
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
)
