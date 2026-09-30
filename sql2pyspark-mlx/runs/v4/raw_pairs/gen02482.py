from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(~F.col("page").isin(["/checkout", "/home", "/product"]) & ((F.col("event_type") == "click") | (F.col("page") == "/search")))
)

summary = (
    base
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    summary
    .select("event_month", "pct_cart")
    .orderBy("pct_cart")
)
