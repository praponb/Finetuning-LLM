from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/product") & F.col("page").isin(["/search", "/checkout", "/product"]))
    .select("event_type", "session_id", "page")
    .orderBy(F.col("event_type").desc(), "session_id")
)
