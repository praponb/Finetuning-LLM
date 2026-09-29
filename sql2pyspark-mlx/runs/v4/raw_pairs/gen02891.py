from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

result = (
    with_last
    .select(
        "id",
        "device",
        "browser",
        "country",
        "event_type",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("device_zone"),
    )
)
