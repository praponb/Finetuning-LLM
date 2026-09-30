from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").isNotNull())
    .select("opened_date", F.lit("store").alias("record_type"))
    .union(
        df_subscriptions
        .filter(F.col("end_date").isNotNull())
        .select("end_date", F.lit("subscription").alias("record_type"))
    )
    .orderBy("opened_date")
)
