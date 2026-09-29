from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("page").eqNullSafe(F.lit("/cart"))) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.concat("page", F.lit(" "), "event_type").alias("page_event_type"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "page",
        "event_time",
        "page_event_type",
        "page_filled",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 7, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["island_id_tier", "island_id"], ["island_id_tier"]],
        "island_id_tier",
        "island_id",
    )
    .agg(
        F.min("event_time").alias("first_seen"),
        F.count("*").alias("row_count"),
        F.min("event_time").alias("started_at"),
    )
    .orderBy(F.col("island_id_tier").desc_nulls_first())
)
