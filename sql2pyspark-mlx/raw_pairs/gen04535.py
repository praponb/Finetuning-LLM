from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .select("id", "session_id")
    .intersect(
        df_web_events
        .filter(~F.col("page").isin(["/home", "/cart"]))
        .select("id", "session_id")
    )
)
