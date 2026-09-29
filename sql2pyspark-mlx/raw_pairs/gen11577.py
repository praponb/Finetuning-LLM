from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.col("signup_date").alias("event_date"), F.lit("customer").alias("origin"))
    .unionByName(
        df_subscriptions
        .filter(F.year("start_date") == 2025)
        .select(
        F.col("end_date").alias("event_date"),
        F.lit("subscription").alias("origin"),
    )
    )
)
