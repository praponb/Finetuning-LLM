from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("site_zone").orderBy(F.col("session_no").desc(), F.col("site_id").desc(), F.col("session_no").desc())

recent = df_meter_readings.filter(F.col("quality") != "OK")

with_prev = recent.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_site_id_running))

session_summary = (
    sessionized
    .groupBy("site_id", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched_site = (
    session_summary.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.site_id",
        "s.session_no",
        "s.session_start",
        "s.session_end",
        "s.event_count",
        "s.session_kwh",
        "s.duration_min",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

unique_rows = (
    enriched_site
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .filter(F.col("event_count") < 4)
    .select(
        "site_id",
        "session_start",
        "session_end",
        "event_count",
        "session_kwh",
        "duration_min",
        "site_commissioned_on",
        "site_name",
        "site_zone",
        F.date_add(F.col("site_commissioned_on"), 90).alias("site_commissioned_on_plus_90d"),
    )
    .orderBy(F.col("session_kwh").desc())
)
