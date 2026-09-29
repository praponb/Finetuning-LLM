from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/search"]))
    .select("id", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") > 433)
        .select("id", "session_id")
    )
    .distinct()
    .orderBy("id")
)
