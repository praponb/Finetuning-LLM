from pyspark.sql import functions as F

result = (
    df_web_events
    .select("session_id", "id")
    .exceptAll(
        df_web_events
        .filter(F.col("page") == "/search")
        .select("session_id", "id")
    )
)
