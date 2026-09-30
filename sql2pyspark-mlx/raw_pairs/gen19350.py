from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_windows = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
    )
)

with_windows_sessions = (
    with_windows
    .filter(F.col("customer_id") == 14)
    .select(
        "id",
        "device",
        "started_at",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

kept = (
    with_windows_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page").isin(["/checkout", "/home"])), "left_semi")
)

with_latest = (
    kept
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.started_at",
        "s.event_seq",
        "s.row_seq",
        F.col("c.page"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

result = (
    with_latest
    .cube(
        "page",
        F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.max("event_seq").alias("max_event_seq"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("event_seq_per_product"),
        F.count(F.when(F.col("web_event_event_type") == "search", 1)).alias("search_rows"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("page") == "/home", 1)), F.lit(0)), 2).alias("event_seq_per_home"),
    )
    .orderBy(F.col("search_rows").desc())
)
