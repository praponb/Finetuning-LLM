from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(237, 369))
    .select("id", "session_id")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([11, 20, 24, 31]))
        .select("id", "session_id")
    )
)
