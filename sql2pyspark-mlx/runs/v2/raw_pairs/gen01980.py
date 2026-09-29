from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/product", "/home"]))
    .select("id", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") == "purchase")
        .select("id", "event_type")
    )
)
