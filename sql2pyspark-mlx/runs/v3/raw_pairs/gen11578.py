from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(
        F.col("start_date").alias("event_date"),
        F.lit("subscription").alias("source"),
    )
    .unionByName(
        df_customers
        .select(F.col("signup_date").alias("event_date"), F.lit("customer").alias("source"))
    )
    .orderBy("source")
)
