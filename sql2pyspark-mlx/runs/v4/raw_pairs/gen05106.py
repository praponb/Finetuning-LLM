from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

result = (
    with_latest
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .groupingSets(
        [["device", "week_start", "activity_month"], ["device", "week_start"], ["device"], []],
        "device",
        "week_start",
        "activity_month",
    )
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.countDistinct("country").alias("distinct_countries"),
        F.size(F.collect_set("device")).alias("num_devices_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("num_devices") > 1)
)
