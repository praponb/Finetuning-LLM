from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
    )
    .filter(F.col("home_count") < 5)
    .drop("home_count")
    .orderBy(F.col("avg_duration_sec").desc())
)
