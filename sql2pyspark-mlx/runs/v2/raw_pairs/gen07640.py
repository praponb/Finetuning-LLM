from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.year("end_date") >= 2023)
        .select("status", F.lit("subscription").alias("source"))
    )
)
