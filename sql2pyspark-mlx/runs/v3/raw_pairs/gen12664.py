from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("page") != "/product")

agg = (
    valid_web_events
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
)

result = agg.select("page", "pct_purchase")
