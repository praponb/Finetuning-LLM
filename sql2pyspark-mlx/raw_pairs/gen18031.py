from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 18)
    .select(F.lit("subscription").alias("kind"), "end_date")
    .unionByName(
        df_accounts
        .select(F.lit("account").alias("kind"), F.col("opened_date").alias("end_date"))
    )
    .distinct()
)
