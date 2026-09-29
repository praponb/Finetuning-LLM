from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select("amount", F.lit("payment").alias("source"))
    .union(
        df_subscriptions
        .filter(F.col("end_date") < "2023-03-09")
        .select("monthly_fee", F.lit("subscription").alias("source"))
    )
    .orderBy("source")
)
