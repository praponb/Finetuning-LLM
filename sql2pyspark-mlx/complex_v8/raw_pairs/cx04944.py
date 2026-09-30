from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_read_at_month_by_rea = Window.partitionBy(F.month("reading_read_at")).orderBy(F.col("reading_kwh").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reading_read_at_month_by_rea_2 = Window.partitionBy(F.month("reading_read_at")).orderBy(F.col("reading_kwh").desc(), "id", "reading_id")
w_reading_id_by_reading_read_a = Window.partitionBy("reading_id").orderBy("reading_read_at", "id", "reading_id")
w_reading_id = Window.partitionBy("reading_id")
w_reading_read_at_month_by_rea_3 = Window.partitionBy(F.month("reading_read_at")).orderBy(F.col("reading_kwh").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_sites = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("H%"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
    )
)

exploded_detail = (
    scoped_sites.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.site_name",
        "s.installed_kw",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"))
)

profiled = (
    exploded_detail.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.avg_reading_voltage")
)

with_windows = (
    profiled
    .select(
        "id",
        "installed_kw",
        "reading_id",
        "reading_read_at",
        "reading_quality",
        "reading_kwh",
        "avg_reading_voltage",
        F.sum("avg_reading_voltage").over(w_reading_read_at_month_by_rea).alias("running_avg_reading_voltage"),
        F.lag("reading_kwh", 1, 0).over(w_reading_read_at_month_by_rea_2).alias("prev_reading_kwh"),
        F.lead("installed_kw").over(w_reading_id_by_reading_read_a).alias("next_installed_kw"),
        F.round(F.col("reading_kwh") - F.avg("reading_kwh").over(w_reading_id), 2).alias("reading_kwh_vs_reading_id_avg"),
        F.first("reading_kwh").over(w_reading_read_at_month_by_rea_3).alias("first_reading_kwh"),
        (F.col("reading_kwh") - F.lag("reading_kwh").over(w_reading_read_at_month_by_rea_2)).alias("reading_kwh_change"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "installed_kw",
        "reading_id",
        "reading_quality",
        "reading_kwh",
        "avg_reading_voltage",
        "prev_reading_kwh",
        "next_installed_kw",
        "reading_kwh_vs_reading_id_avg",
        "first_reading_kwh",
        F.initcap(F.trim(F.col("reading_quality"))).alias("reading_quality_clean"),
    )
    .orderBy("reading_kwh", F.col("id").asc_nulls_last())
)
