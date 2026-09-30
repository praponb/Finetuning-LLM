from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2024-02-18 07:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.concat_ws("-", "country", "browser").alias("country_browser_key"),
        F.substring("browser", 1, 4).alias("browser_prefix"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.page").alias("web_event_page"))
)
