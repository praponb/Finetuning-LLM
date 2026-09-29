from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/home"]))
    .groupBy("page", F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
    )
    .filter(F.col("duration_sec_count") >= 6)
)
