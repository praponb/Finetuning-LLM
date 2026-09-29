from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.substring("device", 1, 2).alias("device_prefix"),
    )
)

result = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.hour(F.col("c.event_time")).between(8, 16)), "left_anti")
)
