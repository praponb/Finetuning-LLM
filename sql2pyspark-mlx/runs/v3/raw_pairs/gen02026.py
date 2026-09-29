from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") != "/cart") & F.hour("event_time").between(10, 14))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
    .orderBy(F.col("home_duration_sec").desc())
)
