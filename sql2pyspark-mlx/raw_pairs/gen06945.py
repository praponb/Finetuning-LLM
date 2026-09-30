from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("page").isin(["/checkout", "/search"]), "tier_1").otherwise("other").alias("page_group"),
        F.quarter("event_time").alias("event_quarter"),
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
    )
)
