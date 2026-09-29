from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("page", "session_id")
    .intersect(
        df_web_events
        .filter(F.col("session_id") == 19)
        .select("page", "session_id")
    )
    .orderBy(F.col("page").desc())
)
