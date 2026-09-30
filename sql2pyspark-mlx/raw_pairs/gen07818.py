from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/cart"))))
    .select(
        "*",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

result = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "session_id", "duration_sec", "event_time_label")
)
