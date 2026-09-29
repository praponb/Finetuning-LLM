from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") > 350)
    .select("page", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("page").isin(["/product", "/search", "/cart"]))
        .select("page", "event_type")
    )
    .distinct()
)
