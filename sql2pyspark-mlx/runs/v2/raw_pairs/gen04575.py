from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([18, 25]))
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
    .drop("cnt")
    .orderBy(F.col("session_id").desc())
)
