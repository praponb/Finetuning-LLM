from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNull() | F.col("page").isin(["/search", "/product", "/cart"]))
    .select("id", "event_type", "session_id", "page")
    .orderBy(F.col("session_id").desc(), "event_type")
)
