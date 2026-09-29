from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "search", "add_to_cart"]))
    .groupBy("page")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") > 210)
    .select("page", "max_duration_sec")
    .orderBy("page")
)
