from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .select("id", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([4, 35]))
        .select("id", "duration_sec")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
