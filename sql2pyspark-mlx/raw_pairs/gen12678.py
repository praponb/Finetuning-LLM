from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isNotNull())
    .select(F.lit("store").alias("record_type"), "city")
    .union(
        df_subscriptions
        .filter(F.col("plan").isNotNull())
        .select(F.lit("subscription").alias("record_type"), "plan")
    )
)
