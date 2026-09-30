from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([9, 27, 38]))
    .select("id", "duration_sec")
    .intersect(
        df_web_events
        .filter(F.col("page").isin(["/product", "/home", "/search"]))
        .select("id", "duration_sec")
    )
    .orderBy("duration_sec")
)
