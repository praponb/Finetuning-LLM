from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/checkout"]) & F.col("event_type").isin(["purchase", "add_to_cart"]))
    .groupBy(
        "page",
        F.date_trunc("MONTH", "event_time").alias("event_month"),
        "event_type",
    )
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
