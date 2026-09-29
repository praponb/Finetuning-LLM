from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/product", "/home"]) & (F.col("duration_sec") == 234))
    .groupBy(F.month("event_time").alias("event_month"), "page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
)
