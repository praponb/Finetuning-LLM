from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(360, 443))
    .select("id", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([3, 11, 25]))
        .select("id", "session_id")
    )
    .distinct()
)
