from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 216)
    .withColumn("page_group", F.when(F.col("page").isin(["/search", "/product"]), "Group A").otherwise("Other"))
    .withColumn("page_event_type_key", F.concat_ws("-", "page", "event_type"))
    .orderBy("duration_sec", F.col("event_time").desc())
)
