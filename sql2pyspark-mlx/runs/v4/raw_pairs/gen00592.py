from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("customer_id", "device")
w5 = Window.partitionBy("event_type").orderBy("duration_sec_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.event_type"))
)

filtered = (
    with_last
    .filter((F.col("started_at") > F.lit("2023-09-05 07:00:00").cast("timestamp")) & F.col("event_type").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "seq_no",
        "duration_sec",
        "event_type",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
        F.lower("event_type").alias("event_type_lower"),
    )
)

enriched_sessions = (
    filtered
    .select(
        "*",
        F.count("duration_sec").over(w4).alias("customer_id_count_duration_sec"),
        F.sum("duration_sec").over(w5).alias("running_duration_sec"),
        F.sum("duration_sec").over(w6).alias("running_duration_sec_2"),
    )
)

summary = (
    enriched_sessions
    .groupBy("customer_id")
    .agg(
        F.max(F.when(F.col("device") == "mobile", F.col("running_duration_sec_2"))).alias("mobile_max_running_duration_sec_2"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
    .filter(F.col("mobile_rows") > 1)
)

result = (
    summary
    .select(
        "customer_id",
        "mobile_max_running_duration_sec_2",
        "event_type_list",
        "duration_sec_p50",
        "mobile_rows",
    )
)
