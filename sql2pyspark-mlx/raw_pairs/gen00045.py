from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser", "device").orderBy("started_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "browser").orderBy("started_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("started_at_date", "id")
w5 = Window.partitionBy(F.year("started_at_date"), "browser").orderBy(F.col("started_at_date").desc(), "id")
w6 = Window.partitionBy("browser").orderBy("started_at_date", "id")

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-06-22").cast("date"))
    .select("*", F.lower("device").alias("device_lower"))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

base = (
    latest_match
    .filter((F.col("started_at") > F.lit("2023-04-19 12:00:00").cast("timestamp")) & (F.col("country") == "US"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.to_date("started_at").alias("started_at_date"),
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

with_windows = (
    base
    .filter(F.col("browser").isin(["Firefox", "Edge"]))
    .select(
        "id",
        "device",
        "started_at_label",
        "browser_clean",
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser"),
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w5).alias("seq_no"),
        F.row_number().over(w6).alias("event_seq_2"),
    )
)

result = (
    with_windows
    .rollup(
        F.when(F.col("event_seq") >= 6, "HIGH").otherwise("LOW").alias("event_seq_tier"),
        "device",
        F.when(F.col("seq_no") >= 8, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.max(F.when(F.col("device") == "desktop", F.col("event_seq"))).alias("desktop_max_event_seq"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("device") == "desktop", F.col("event_seq_2")).otherwise(0)).alias("desktop_event_seq_2"),
        F.sum(F.when(F.col("device") == "desktop", F.col("event_seq_2"))).alias("desktop_total_event_seq_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
