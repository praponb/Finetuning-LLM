from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.country").isin(["SG", "TH"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"))
)
