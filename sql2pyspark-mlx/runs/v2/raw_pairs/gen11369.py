from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .select("duration_sec")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([1, 8]))
        .select("duration_sec")
    )
)
