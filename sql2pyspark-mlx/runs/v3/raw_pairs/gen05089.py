from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(353, 355) & F.col("duration_sec").between(268, 305))
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 366, "high").when(F.col("duration_sec") >= 137, "mid").otherwise("low").alias("duration_sec_bucket"),
        "event_type",
    )
    .agg(F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"))
    .filter(F.col("home_count") < 1)
)
