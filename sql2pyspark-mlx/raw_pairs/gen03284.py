from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isNotNull())
    .select("city")
    .union(
        df_web_events
        .filter(F.col("event_type").isNotNull())
        .select("event_type")
    )
    .orderBy("city")
)
