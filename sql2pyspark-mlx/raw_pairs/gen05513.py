from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("event_time").desc())
w4 = Window.partitionBy("event_type").orderBy("event_time", "id")
w5 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").eqNullSafe(F.lit("/cart")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round((F.col("duration_sec") * 0.9), 2).alias("duration_sec_adjusted"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "page_filled",
        "page_clean",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

latest = (
    islands
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    latest
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands_web_events = (
    flagged
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "page_clean",
        "rank_in_group",
        F.sum("is_new_burst").over(w5).alias("island_id_2"),
    )
)

result = (
    islands_web_events
    .select("id", "event_type", "page", "rank_in_group")
    .orderBy(F.col("rank_in_group").desc())
)
