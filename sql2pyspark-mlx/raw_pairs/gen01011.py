from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.col("device") != "tablet"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("country").alias("country_length"),
        F.when(F.col("device") == "desktop", 1).otherwise(0).alias("is_desktop"),
        F.coalesce("browser", F.lit("OTHER")).alias("resolved_browser"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        "s.country",
        "s.country_length",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.page"),
    )
)
