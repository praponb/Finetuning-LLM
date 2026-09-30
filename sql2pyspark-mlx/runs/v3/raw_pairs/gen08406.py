from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/cart"]))
    .groupBy("event_type")
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
    )
    .filter(F.col("home_duration_sec") > 54)
)
