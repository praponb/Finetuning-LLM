from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 218)
    .select("id", "duration_sec")
    .subtract(
        df_web_events
        .filter(F.col("event_type").isin(["click", "add_to_cart"]))
        .select("id", "duration_sec")
    )
)
