from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") != "/checkout")
    .select("page", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("page") == "/cart")
        .select("page", "event_type")
    )
    .orderBy("event_type")
)
