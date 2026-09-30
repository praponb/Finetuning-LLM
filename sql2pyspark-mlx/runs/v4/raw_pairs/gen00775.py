from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/cart"]))
    .select("event_type", "page")
    .unionByName(
        df_web_events
        .filter(F.to_date("event_time") < "2024-12-01")
        .select("event_type", "page")
    )
    .orderBy("event_type")
)
