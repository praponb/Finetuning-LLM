from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter(F.col("started_at") < "2023-10-05")
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("NONE")))
)

result = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_time").alias("web_event_event_time"),
    )
)
