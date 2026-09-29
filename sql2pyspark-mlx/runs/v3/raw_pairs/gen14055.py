from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["purchase", "add_to_cart"]))
    .select("session_id")
    .unionByName(
        df_web_events
        .filter(~F.col("event_type").isin(["search", "add_to_cart"]))
        .select("session_id")
    )
    .orderBy(F.col("session_id").desc())
)
