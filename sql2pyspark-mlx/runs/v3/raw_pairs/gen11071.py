from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select("page")
    .intersect(
        df_web_events
        .filter(F.to_date("event_time") < "2023-12-01")
        .select("page")
    )
    .orderBy("page")
)
