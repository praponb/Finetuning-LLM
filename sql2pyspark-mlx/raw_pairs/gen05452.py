from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("event_time", "id").rowsBetween(-2, -1)
w_part = Window.partitionBy("event_type").orderBy(F.col("duration_sec_num").desc(), "id")
w4 = Window.partitionBy("session_id").orderBy("duration_sec_num_avg_2_prior", "id")
w5 = Window.partitionBy("page").orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("event_time_day"), "browser").orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device").orderBy("duration_sec_num_avg_2_prior_bucket")
w8 = Window.partitionBy("device", "browser")

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-05-24").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

enriched = (
    filtered
    .withColumn("cumulative_duration_sec_num", F.sum("duration_sec_num").over(w))
    .withColumn("duration_sec_num_avg_2_prior", F.avg("duration_sec_num").over(w2))
    .withColumn("event_seq", F.row_number().over(w_part))
)

cleaned = (
    enriched
    .filter(F.col("event_time") >= F.lit("2023-09-30").cast("date"))
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        "event_type_title",
        "duration_sec_num",
        "resolved_page",
        "cumulative_duration_sec_num",
        "duration_sec_num_avg_2_prior",
        F.col("event_time").cast("date").alias("event_time_day"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("duration_sec") <= 402)
    .select(
        "*",
        F.ntile(3).over(w4).alias("duration_sec_num_avg_2_prior_bucket"),
        F.sum("duration_sec").over(w5).alias("running_duration_sec"),
    )
)

enriched_web_events = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.device"), F.col("p.browser"))
)

enriched_web_events_2 = (
    enriched_web_events
    .select(
        "*",
        F.sum("duration_sec_num").over(w6).alias("running_duration_sec_num"),
        F.cume_dist().over(w7).alias("duration_sec_num_avg_2_prior_bucket_cume_dist"),
        F.round((F.col("duration_sec_num_avg_2_prior_bucket") / F.sum("duration_sec_num_avg_2_prior_bucket").over(w8)), 4).alias("duration_sec_num_avg_2_prior_bucket_share"),
    )
)

result = (
    enriched_web_events_2
    .groupBy("session_id", "device")
    .pivot("browser", ["Chrome", "Firefox"])
    .agg(F.sum("running_duration_sec_num").alias("amount"))
)
