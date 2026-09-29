from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_plus_15m_year_burst = Window.partitionBy(F.year("read_at_plus_15m"), "burst_id")

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_plus_15m"),
    )
)

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_quality_running))

windowed = (
    sessionized
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "burst_id",
        F.count(F.lit(1)).over(w_read_at_plus_15m_year_burst).alias("rows_in_read_at_plus_15m_year_burst_id"),
        F.size(F.collect_set("quality").over(w_read_at_plus_15m_year_burst)).alias("distinct_quality_in_read_at_plus_15m_year_burst_"),
        F.round(F.col("burst_id") / F.nullif(F.sum("burst_id").over(w_read_at_plus_15m_year_burst), F.lit(0)), 4).alias("burst_id_share_of_read_at_plus_15m_year_burst_id"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.burst_id",
        "s.rows_in_read_at_plus_15m_year_burst_id",
        "s.distinct_quality_in_read_at_plus_15m_year_burst_",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "burst_id",
        "rows_in_read_at_plus_15m_year_burst_id",
        "distinct_quality_in_read_at_plus_15m_year_burst_",
        "site_tariff",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        F.length(F.trim(F.col("site_tariff"))).alias("site_tariff_len"),
    )
    .orderBy(F.col("rows_in_read_at_plus_15m_year_burst_id").asc_nulls_last())
)
