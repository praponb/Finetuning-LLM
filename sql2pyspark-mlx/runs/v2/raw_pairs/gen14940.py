from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 344)
    .select("page")
    .distinct()
    .orderBy("page")
    .limit(10)
)
