from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-08-23 15:00:00"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.started_at", "s.browser_filled", F.col("c.event_time"))
)
