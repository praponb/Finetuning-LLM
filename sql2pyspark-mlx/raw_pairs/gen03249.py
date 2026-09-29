from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_first(), "id")
w3 = Window.orderBy("dedup_rank", "id")
w4 = Window.partitionBy("country").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_windows = (
    df_sessions
    .filter(F.col("customer_id").isin([19, 28]))
    .select(
        "id",
        "started_at",
        "country",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "last_known_browser",
        "dedup_rank",
        F.row_number().over(w3).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dedup_rank",
        "s.first_known_country",
        F.col("c.page"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

result = (
    latest_match
    .groupBy("page")
    .agg(
        F.count(F.when(F.col("web_event_event_type") == "page_view", 1)).alias("page_view_count"),
        F.countDistinct("web_event_event_type").alias("distinct_web_event_event_types"),
        F.size(F.collect_set("web_event_event_type")).alias("num_web_event_event_types"),
    )
    .filter(F.col("distinct_web_event_event_types") < 1000)
)
