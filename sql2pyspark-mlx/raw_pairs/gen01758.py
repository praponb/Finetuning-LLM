from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([37, 38]))
    .select("id", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([1, 7, 10, 23]))
        .select("id", "session_id")
    )
)
