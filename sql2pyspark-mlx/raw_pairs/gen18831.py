from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 162)
    .select("id", "page")
    .orderBy("page", "id")
    .limit(5)
)
