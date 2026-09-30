from pyspark.sql import functions as F

result = (
    df_web_events
    .select("session_id", "page")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") > 123)
        .select("session_id", "page")
    )
)
