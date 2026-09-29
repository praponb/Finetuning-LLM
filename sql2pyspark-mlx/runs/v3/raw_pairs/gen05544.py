from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 203)
    .select("page", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") >= 471)
        .select("page", "duration_sec")
    )
    .distinct()
    .orderBy("duration_sec")
)
