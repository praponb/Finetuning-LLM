from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 222) & (F.col("page") == "/checkout") & F.col("page").isin(["/product", "/search", "/checkout"]))
    .groupBy("page", "event_type")
    .agg(F.sort_array(F.collect_set("event_type")).alias("event_type_list"))
    .orderBy("event_type", F.col("page").desc())
)
