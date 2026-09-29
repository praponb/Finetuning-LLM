from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.orderBy(F.col("started_at").desc(), "id")
w3 = Window.orderBy("started_at", "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("device", "web_event_event_type").orderBy(F.col("is_new_burst").desc(), "id")
w7 = Window.partitionBy("device", "web_event_event_type").orderBy(F.col("row_seq_3").asc_nulls_first(), "id")

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-12-12").cast("date")) & F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "started_at",
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
        F.coalesce("country", F.lit("N/A")).alias("resolved_country"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_semi")
)

scored = (
    kept
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("row_seq_2", F.row_number().over(w2))
    .withColumn("row_seq_3", F.row_number().over(w3))
)

with_gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w4))

latest_match = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

ranked = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

totals = (
    latest
    .cube("island_id", "device", F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.count(F.when(F.col("web_event_event_type") == "click", 1)).alias("click_rows"),
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.countDistinct("device").alias("distinct_devices"),
        F.sum(F.when(F.col("device") == "tablet", F.col("row_seq")).otherwise(0)).alias("tablet_row_seq"),
        F.sum(F.when(F.col("device") == "mobile", F.col("row_seq_3")).otherwise(0)).alias("mobile_row_seq_3"),
    )
)

result = (
    totals
    .select("device", "click_rows", "cnt", "tablet_row_seq", "mobile_row_seq_3")
    .orderBy("cnt")
)
