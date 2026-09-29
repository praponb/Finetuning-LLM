from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~(~F.col("event_type").isin(["add_to_cart", "purchase", "click"]) | (F.col("event_type") == "click")))
    .groupBy("page")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") > 21)
    .orderBy("avg_duration_sec")
)
