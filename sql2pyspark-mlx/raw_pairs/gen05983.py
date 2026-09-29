from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select(F.lit("customer").alias("record_type"), "signup_date")
    .union(
        df_subscriptions
        .filter(F.col("start_date").isNotNull())
        .select(F.lit("subscription").alias("record_type"), "start_date")
    )
)
