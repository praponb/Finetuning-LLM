from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/home", "/search"]))
    .select("page", "event_type")
    .distinct()
    .orderBy("event_type")
)
