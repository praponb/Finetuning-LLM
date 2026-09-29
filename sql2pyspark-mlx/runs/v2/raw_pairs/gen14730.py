from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "page_view")
    .select("session_id")
    .unionByName(
        df_web_events
        .filter(F.col("event_type").isin(["search", "click", "add_to_cart"]))
        .select("session_id")
    )
    .distinct()
)
