from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.started_at", F.col("c.page"), F.col("c.duration_sec"))
)

result = (
    latest_match
    .rollup(F.date_trunc("YEAR", "started_at").alias("year_start"), "page")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
        F.count("*").alias("cnt_2"),
    )
)
