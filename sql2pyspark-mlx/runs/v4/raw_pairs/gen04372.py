from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w_hist = Window.partitionBy("event_type", "page").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type", "page").orderBy(F.col("duration_sec").desc())
w5 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("running_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device").orderBy("running_duration_sec", "id")
w8 = Window.partitionBy("device").orderBy(F.col("duration_sec_count_90d").desc(), "id")
w9 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w10 = Window.partitionBy("quarter_start").orderBy(F.col("th_rows").desc())

latest = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    latest
    .withColumn("prev_duration_sec", F.lag("duration_sec", 1).over(w_hist))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w3))
    .withColumn("duration_sec_rank", F.rank().over(w4))
    .withColumn("duration_sec_count_90d", F.count("duration_sec").over(w5))
)

joined = (
    metrics
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.browser"), F.col("p.device"), F.col("p.country"))
)

metrics_web_events = (
    joined
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w6).alias("last_known_browser"),
        F.lag("prev_duration_sec", 1).over(w7).alias("prev_prev_duration_sec"),
        F.row_number().over(w8).alias("event_seq"),
        F.avg("duration_sec_count_90d").over(w9).alias("duration_sec_count_90d_avg_90d"),
    )
)

result = (
    metrics_web_events
    .groupBy(
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        F.date_trunc("WEEK", "event_time").alias("week_start"),
    )
    .agg(
        F.percentile_approx("running_duration_sec", [0.25, 0.5, 0.75]).alias("running_duration_sec_quartiles"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
        F.avg(F.when(F.col("browser") == "Chrome", F.col("duration_sec_count_90d"))).alias("chrome_avg_duration_sec_count_90d"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("search_rows") >= 1)
    .withColumn("th_rows_dense_rank", F.dense_rank().over(w10))
    .orderBy(F.col("quarter_start").desc())
)
