from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 19))
    .groupBy("event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
    )
    .orderBy(F.col("pct_cart").desc())
)
