from pyspark.sql import functions as F

min_duration_sec = df_web_events.filter(F.col("event_type").isin(["page_view", "search", "add_to_cart"])).agg(F.min("duration_sec")).first()[0]

result = (
    df_web_events
    .filter(F.col("duration_sec") <= min_duration_sec)
    .select("id", "duration_sec")
    .orderBy(F.col("id").desc())
)
