from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("device", "web_event_page").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w8 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("web_event_event_type_2").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy(F.year("started_at"), "web_event_event_type").orderBy(F.col("last_known_event_seq").desc(), "id")
w11 = Window.partitionBy("web_event_page").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        "s.country",
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec") != 294), "left_anti")
)

with_windows = (
    kept
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

latest_match_sessions = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.event_type").alias("web_event_event_type_2"),
    )
)

metrics = (
    latest_match_sessions
    .select(
        "*",
        F.count("event_seq").over(w5).alias("event_seq_count_90d"),
        F.sum("event_seq").over(w6).alias("event_seq_sum_7d"),
        F.row_number().over(w7).alias("row_seq"),
        F.last("event_seq", ignorenulls=True).over(w8).alias("last_known_event_seq"),
        F.last("browser", ignorenulls=True).over(w_grp).alias("last_known_browser"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec") <= 266), "left_anti")
)

result = (
    not_flagged
    .filter(F.col("row_seq").between(3, 8))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "web_event_event_type",
        "first_known_country",
        "event_seq_count_90d",
        "event_seq_sum_7d",
        "row_seq",
        "last_known_event_seq",
        "last_known_browser",
        F.ntile(10).over(w10).alias("last_known_event_seq_bucket"),
        F.sum("event_seq_count_90d").over(w11).alias("running_event_seq_count_90d"),
    )
)
