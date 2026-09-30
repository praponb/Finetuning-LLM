from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "session_id", F.upper("page").alias("page_upper"))
    .orderBy(F.col("session_id").desc(), "id")
    .limit(3)
)
