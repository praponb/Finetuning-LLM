from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
    .orderBy(F.col("session_id").desc())
    .limit(20)
)
