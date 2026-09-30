from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 15]) & F.col("session_id").isin([16, 31]))
    .groupBy("page", "event_type")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
)
