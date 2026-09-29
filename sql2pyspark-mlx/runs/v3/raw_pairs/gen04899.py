from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "bank_transfer"]))
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date").between("2023-03-01", "2023-04-01"))
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
