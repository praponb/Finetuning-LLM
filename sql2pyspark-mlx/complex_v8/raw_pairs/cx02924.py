from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_read_at_quar = Window.partitionBy(F.to_date("read_at"), "read_at_quarter")
w_read_at_quarter_by_read_at_q = Window.partitionBy("read_at_quarter").orderBy("read_at_quarter", "id")

valid = (
    df_meter_readings
    .filter((F.col("voltage") < 234.7) & (F.col("read_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.abs(F.col("voltage") - 226.0).alias("voltage_distance"),
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_read_at_to_date_read_at_quar)).alias("distinct_site_id_in_read_at_to_date_read_at_quar"),
        F.max("voltage").over(w_read_at_to_date_read_at_quar).alias("read_at_to_date_read_at_quarter_max_voltage"),
        F.rank().over(w_read_at_quarter_by_read_at_q).alias("read_at_quarter_rank"),
        F.round(F.col("read_at_quarter") - F.avg("read_at_quarter").over(w_read_at_to_date_read_at_quar), 2).alias("read_at_quarter_vs_read_at_to_date_read_at_quart"),
        F.min("kwh").over(w_read_at_to_date_read_at_quar).alias("read_at_to_date_read_at_quarter_min_kwh"),
    )
)

with_activity = (
    with_trends.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 116.8), "left_anti")
    .filter(F.col("distinct_site_id_in_read_at_to_date_read_at_quar") >= 2)
)

with_parent_site = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.voltage_distance",
        "s.read_at_quarter",
        "s.distinct_site_id_in_read_at_to_date_read_at_quar",
        "s.read_at_to_date_read_at_quarter_max_voltage",
        "s.read_at_quarter_rank",
        "s.read_at_quarter_vs_read_at_to_date_read_at_quart",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

detail_rows_readings = (
    with_parent_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.distinct_site_id_in_read_at_to_date_read_at_quar",
        "s.read_at_to_date_read_at_quarter_max_voltage",
        "s.read_at_quarter_vs_read_at_to_date_read_at_quart",
        "s.site_installed_kw",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

result = (
    detail_rows_readings
    .groupingSets(
        [["distinct_site_id_in_read_at_to_date_read_at_quar", "reading_quality"], ["distinct_site_id_in_read_at_to_date_read_at_quar"], ["reading_quality"]],
        "distinct_site_id_in_read_at_to_date_read_at_quar", "reading_quality",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("reading_kwh"), 2).alias("stddev_reading_kwh"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
    .orderBy(
        F.col("distinct_site_id_in_read_at_to_date_read_at_quar").asc_nulls_last(),
        F.col("reading_quality").asc_nulls_last(),
    )
)
