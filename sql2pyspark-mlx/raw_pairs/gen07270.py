from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status")
    .unionByName(
        df_web_events
        .filter(F.col("page").isNull())
        .select(F.col("page").alias("status"))
    )
)
