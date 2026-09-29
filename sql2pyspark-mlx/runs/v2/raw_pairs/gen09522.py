from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .pivot("page", ["/checkout", "/home", "/product", "/cart"])
    .agg(F.count("duration_sec"))
)
