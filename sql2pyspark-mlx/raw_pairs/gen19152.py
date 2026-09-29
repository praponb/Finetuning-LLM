from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-19 00:00:00").cast("timestamp")) & F.col("session_id").isin([3, 26, 29, 40]))
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("page_title_2", F.initcap(F.trim("page")))
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        "page_title",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessions_web_events
    .rollup("session_id")
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.count("*").alias("row_count"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id_2"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("avg_island_id").desc_nulls_first())
)
