from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("page_length").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("event_time"), "session_id").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("event_type").orderBy("event_time", "id")
w5 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type", "page").orderBy("event_time", "id").rowsBetween(-2, -1)
w7 = Window.orderBy("duration_sec", "id")
w_ordered = Window.partitionBy("session_id", "event_type").orderBy("duration_sec_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.orderBy(F.col("island_id").desc(), "id")
w10 = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy("event_time", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("session_id").orderBy("island_id", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.orderBy(F.col("duration_sec_bucket").desc(), "id")

base = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .select(
        "*",
        F.length("page").alias("page_length"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

enriched = (
    base
    .withColumn("page_length_bucket", F.ntile(3).over(w))
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w2))
    .withColumn("duration_sec_avg_6_prior", F.avg("duration_sec").over(w3))
)

with_gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "page_clean",
        F.sum("session_start_flag").over(w5).alias("island_id"),
    )
)

metrics = (
    sessions_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "page_clean",
        "island_id",
        F.avg("duration_sec").over(w6).alias("duration_sec_avg_2_prior"),
        F.ntile(3).over(w7).alias("duration_sec_bucket"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.sum("duration_sec_avg_2_prior").over(w_ordered).alias("cumulative_duration_sec_avg_2_prior"),
        F.ntile(3).over(w_ordered_2).alias("island_id_bucket"),
        F.avg("island_id").over(w10).alias("island_id_avg_6_prior"),
        F.avg("duration_sec_bucket").over(w11).alias("duration_sec_bucket_avg_2"),
        F.ntile(3).over(w_hist).alias("duration_sec_bucket_bucket"),
    )
)

result = (
    scored
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["device", "event_type"], ["device"], []],
        F.col("p.device").alias("device"),
        F.col("src.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("src.duration_sec_avg_2_prior")), 2).alias("avg_duration_sec_avg_2_prior"),
        F.avg(F.col("src.island_id")).alias("avg_island_id"),
        F.grouping_id().alias("level_id"),
    )
)
