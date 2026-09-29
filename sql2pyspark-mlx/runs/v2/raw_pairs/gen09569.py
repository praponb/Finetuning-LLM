from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .groupBy("session_id")
    .pivot("page", ["/product", "/search"])
    .agg(F.avg("duration_sec"))
)
