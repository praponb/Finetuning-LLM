from pyspark.sql import functions as F

result = (
    df_payments
    .select("status", F.lit("payment").alias("origin"))
    .unionByName(
        df_subscriptions
        .filter(F.year("start_date") == 2024)
        .select("status", F.lit("subscription").alias("origin"))
    )
    .orderBy("status")
)
