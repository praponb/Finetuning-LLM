from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/search", "/product", "/home"]))
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .orderBy(F.col("unique_sessions").desc())
)
