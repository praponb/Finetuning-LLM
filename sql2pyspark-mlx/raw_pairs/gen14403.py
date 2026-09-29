from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        F.col("signup_date").alias("event_date"),
        F.lit("customer").alias("record_type"),
    )
    .unionByName(
        df_subscriptions
        .select(
        F.col("start_date").alias("event_date"),
        F.lit("subscription").alias("record_type"),
    )
    )
)
