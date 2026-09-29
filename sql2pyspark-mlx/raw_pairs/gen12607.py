from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy("country").orderBy("duration_sec", "id")

metrics = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.ntile(4).over(w).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_2"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        "a.event_time",
        "a.duration_sec",
        F.col("b.country"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .filter(~F.col("s.country").isin(["SG", "JP"]))
    .select("src.*", F.col("s.device"), F.col("s.browser"))
)

metrics_web_events = (
    joined
    .select(
        "id",
        "event_time",
        "browser",
        F.lead("duration_sec").over(w_hist).alias("next_duration_sec"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

result = (
    metrics_web_events
    .groupBy(
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.sum(F.when(F.col("browser") == "Safari", F.col("row_seq")).otherwise(0)).alias("safari_row_seq"),
        F.sum(F.when(F.col("browser") == "Firefox", F.col("next_duration_sec")).otherwise(0)).alias("firefox_next_duration_sec"),
        F.sum("row_seq").alias("total_row_seq"),
        F.sum(F.when(F.col("browser") == "Edge", F.col("next_duration_sec"))).alias("edge_total_next_duration_sec"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
    .filter(F.col("firefox_next_duration_sec") < 1000000)
    .orderBy(F.col("total_row_seq").asc_nulls_last())
)
