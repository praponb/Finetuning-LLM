from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-03-01") & (F.to_date("event_time") >= "2023-12-01") & F.col("event_type").isin(["click", "add_to_cart"]))
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 423, "high").when(F.col("duration_sec") >= 91, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.count("duration_sec").alias("duration_sec_count"))
    .filter(F.col("duration_sec_count") >= 3)
)
