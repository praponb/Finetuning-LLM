from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2025-04-01") & (F.col("page") != "/cart"))
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
    .orderBy(F.col("session_id").desc())
)
