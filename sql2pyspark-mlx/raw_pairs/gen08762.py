from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-03-23 02:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "id",
        "browser",
        "country",
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.is_tablet", F.col("c.page"), F.col("c.event_type"))
)
