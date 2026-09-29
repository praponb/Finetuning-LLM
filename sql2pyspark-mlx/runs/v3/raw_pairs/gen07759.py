from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "search", "click"]) & F.col("page").isin(["/checkout", "/cart"]))
    .groupBy(F.hour("event_time").alias("event_hour"), "event_type")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
)
