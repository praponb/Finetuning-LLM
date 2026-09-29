from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([14, 23, 26]))
    .select(F.col("opened_date").alias("event_date"), F.lit("account").alias("kind"))
    .unionByName(
        df_subscriptions
        .select(F.col("end_date").alias("event_date"), F.lit("subscription").alias("kind"))
    )
    .orderBy("kind")
)
