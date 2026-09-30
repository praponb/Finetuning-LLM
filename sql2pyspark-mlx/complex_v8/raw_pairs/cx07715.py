from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_2_read_at_month_by = Window.partitionBy("site_zone_2", F.date_trunc("month", "read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_avg_reading_voltage = Window.orderBy("avg_reading_voltage", "id")
w_all_by_avg_reading_voltage_p = Window.orderBy("avg_reading_voltage", "id").rowsBetween(-2, -1)
w_site_zone_2_read_at_month_by_2 = Window.partitionBy("site_zone_2", F.date_trunc("month", "read_at")).orderBy(F.col("kwh").asc_nulls_last())
w_first_reading_read_at_month = Window.partitionBy(F.date_trunc("month", "first_reading_read_at"), "site_zone")

valid_readings = (
    df_meter_readings
    .filter(F.col("site_id") <= 5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_details_site = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_parent = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone_2"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_counts = (
    with_parent.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_voltage", "m.first_reading_read_at")
)

with_windows = (
    with_counts
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "site_tariff",
        "site_zone_2",
        "site_commissioned_on",
        "site_name",
        "first_reading_read_at",
        F.last("kwh", ignorenulls=True).over(w_site_zone_2_read_at_month_by).alias("last_known_kwh"),
        (F.col("avg_reading_voltage") - F.lag("avg_reading_voltage").over(w_all_by_avg_reading_voltage)).alias("avg_reading_voltage_change"),
        F.round(F.avg("avg_reading_voltage").over(w_all_by_avg_reading_voltage_p), 2).alias("avg_reading_voltage_avg_2_prior"),
        F.round(F.cume_dist().over(w_site_zone_2_read_at_month_by_2), 3).alias("kwh_cume_dist"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_first_reading_read_at_month), 2).alias("kwh_vs_first_reading_read_at_month_site_zone_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "site_tariff",
        "site_zone_2",
        "site_name",
        "first_reading_read_at",
        "last_known_kwh",
        "avg_reading_voltage_avg_2_prior",
        "kwh_vs_first_reading_read_at_month_site_zone_avg",
        (F.col("read_at") + F.expr("INTERVAL 7 DAYS")).alias("read_at_plus_7d"),
        F.substring(F.trim(F.col("site_zone_2")), 1, 3).alias("site_zone_2_prefix"),
    )
    .orderBy(
        F.col("last_known_kwh").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
