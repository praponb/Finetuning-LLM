from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 23)
    .select("page")
    .unionByName(df_web_events.filter(F.col("duration_sec") == 388).select("page"))
    .orderBy("page")
)
