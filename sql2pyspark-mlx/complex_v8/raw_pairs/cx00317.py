from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_session_id_by_site_c = Window.partitionBy("site_id", "session_id").orderBy(F.col("site_commissioned_on").asc_nulls_last(), "site_id", "session_id")
w_site_id_events = Window.partitionBy("site_id", "events")
w_site_id_site_tariff_by_durat = Window.partitionBy("site_id", "site_tariff").orderBy("duration_min", "site_id", "session_id")

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_site_id_running))

session_summary = (
    sessionized
    .groupBy("site_id", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent = (
    session_summary.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.session_id",
        "s.session_start",
        "s.session_end",
        "s.events",
        "s.session_kwh",
        "s.duration_min",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.rank().over(w_site_id_session_id_by_site_c).alias("site_commissioned_on_rank"),
        F.size(F.collect_set("site_tariff").over(w_site_id_events)).alias("distinct_site_tariff_in_site_id_events"),
        F.row_number().over(w_site_id_site_tariff_by_durat).alias("duration_min_seq"),
    )
)

result = (
    with_windows
    .select(
        "site_id",
        "session_id",
        "session_start",
        "session_end",
        "duration_min",
        "site_tariff",
        "site_installed_kw",
        "site_commissioned_on",
        "site_commissioned_on_rank",
        "distinct_site_tariff_in_site_id_events",
        "duration_min_seq",
        (F.col("session_start") + F.expr("INTERVAL 30 MINUTES")).alias("session_start_plus_30m"),
        F.quarter(F.col("site_commissioned_on")).alias("site_commissioned_on_quarter"),
    )
    .orderBy(
        F.col("site_tariff").desc(),
        F.col("site_installed_kw").desc(),
        "site_id",
        "session_id",
    )
    .limit(10)
)
