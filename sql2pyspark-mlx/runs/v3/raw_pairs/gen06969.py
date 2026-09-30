from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("duration_sec") == 453)
    .withColumn("page_lower", F.lower("page"))
)

agg = (
    valid_web_events
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
)

result = agg.orderBy(F.col("session_id").desc())
