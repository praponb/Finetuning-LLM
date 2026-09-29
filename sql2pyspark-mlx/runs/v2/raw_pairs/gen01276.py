from pyspark.sql import functions as F

base = df_web_events.filter(F.col("event_time") < "2023-11-07")

event_type_summary = (
    base
    .groupBy("event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
    )
)

result = event_type_summary.select("event_type", "pct_add_to_cart")
