from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

result = (
    latest_match
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "web_event_event_time",
        F.when(F.col("country").rlike("^(UK|JP)$"), "EXTENDED").when(F.col("country").rlike("^(TH|US)$"), "CORE").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("country").desc_nulls_last())
)
