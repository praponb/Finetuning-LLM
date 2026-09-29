from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("page").orderBy("island_id", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("page").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("island_id", "id")
w7 = Window.partitionBy("page").orderBy("island_id_bucket", "id")

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-07").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.concat("event_type", F.lit(" "), "page").alias("event_type_page"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "event_type_page",
        "page_filled",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

metrics = (
    sessions_web_events
    .filter(F.col("page") == "/search")
    .select(
        "id",
        "page",
        "event_type_page",
        "page_filled",
        "island_id",
        F.ntile(10).over(w_ordered).alias("island_id_bucket"),
        F.first("page", ignorenulls=True).over(w4).alias("first_known_page"),
        F.last("page", ignorenulls=True).over(w5).alias("last_known_page"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "page",
        "event_type_page",
        "page_filled",
        "island_id",
        "island_id_bucket",
        "last_known_page",
        F.ntile(5).over(w6).alias("island_id_bucket_2"),
        F.first("page", ignorenulls=True).over(w5).alias("first_known_page_2"),
        F.ntile(3).over(w7).alias("island_id_bucket_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "page",
        "page_filled",
        "island_id_bucket",
        "last_known_page",
        "island_id_bucket_2",
        "island_id_bucket_bucket",
    )
)
