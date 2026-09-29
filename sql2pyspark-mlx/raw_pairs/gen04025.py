from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

result = (
    with_latest
    .groupingSets(
        [["customer_id", "web_event_page"], ["customer_id"], []],
        "customer_id",
        "web_event_page",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.max("started_at").alias("last_seen"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("event_count")
)
