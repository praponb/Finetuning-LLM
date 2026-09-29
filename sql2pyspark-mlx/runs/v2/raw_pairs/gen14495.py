from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
    )
    .orderBy("page")
)
