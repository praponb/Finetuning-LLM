from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_read_at_month_by = Window.partitionBy("site_tariff", F.date_trunc("month", "read_at")).orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_commissioned_on_year_by = Window.partitionBy(F.year("site_commissioned_on")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_commissioned_on_year_qu = Window.partitionBy(F.year("site_commissioned_on"), "quality_or_unknown")
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_site_commissioned_on_year_by_2 = Window.partitionBy(F.year("site_commissioned_on")).orderBy("kwh", "id")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_quality_or_unknown_read_at_m = Window.partitionBy("quality_or_unknown", F.date_trunc("month", "read_at")).orderBy("voltage", "id")
w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))
w_site_tariff_quality_or_unkno = Window.partitionBy("site_tariff", "quality_or_unknown").orderBy("voltage", "id").rowsBetween(-3, -1)

base = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

joined = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_or_unknown",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_or_unknown",
        "site_commissioned_on",
        "site_tariff",
        "site_installed_kw",
        F.sum("site_installed_kw").over(w_site_tariff_read_at_month_by).alias("running_site_installed_kw"),
        F.nth_value("read_at", 2).over(w_site_commissioned_on_year_by).alias("second_read_at"),
        F.max("kwh").over(w_site_commissioned_on_year_qu).alias("site_commissioned_on_year_quality_or_unknown_max"),
        F.sum("site_installed_kw").over(w_read_at_year).alias("read_at_year_sum_site_installed_kw"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_year), F.lit(0)), 4).alias("voltage_share_of_read_at_year"),
        F.lag("site_installed_kw", 2).over(w_site_commissioned_on_year_by_2).alias("prev2_site_installed_kw"),
        F.max("voltage").over(w_site_commissioned_on_year_qu).alias("site_commissioned_on_year_quality_or_unknown_max_2"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.lag("voltage", 2).over(w_quality_or_unknown_read_at_m).alias("prev2_voltage"),
        F.ntile(10).over(w_all_by_voltage).alias("voltage_decile"),
        F.round(F.col("site_installed_kw") / F.nullif(F.sum("site_installed_kw").over(w_read_at_month), F.lit(0)), 4).alias("site_installed_kw_share_of_read_at_month"),
        (F.col("voltage") - F.lag("voltage").over(w_site_commissioned_on_year_by_2)).alias("voltage_change"),
        F.round(F.avg("kwh").over(w_site_tariff_quality_or_unkno), 2).alias("kwh_avg_3_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "site_commissioned_on",
        "site_installed_kw",
        "running_site_installed_kw",
        "second_read_at",
        "read_at_year_sum_site_installed_kw",
        "voltage_share_of_read_at_year",
        "prev2_site_installed_kw",
        "site_commissioned_on_year_quality_or_unknown_max_2",
        "minutes_since_prev_read_at",
        "voltage_decile",
        "site_installed_kw_share_of_read_at_month",
        "voltage_change",
    )
    .orderBy("site_installed_kw")
)
