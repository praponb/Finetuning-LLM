from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("id", "page", "duration_sec")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([26, 40]))
        .select("id", "page", "duration_sec")
    )
)
